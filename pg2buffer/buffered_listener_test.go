package pg2buffer

import (
	"context"
	"fmt"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest/testpgreplicator"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"testing"
	"time"
)

// SELECT pg_is_in_recovery();
const (
	pgImage = "docker.io/postgres:16.3"
	// dbName      = "pgreplication"
	userName  = "postgres"
	password  = "password"
	tableName = "repl_table"
	slotName  = "repl_slot"
	pubName   = "repl_pub"
)

type downstreamDownError struct {
}

func (d downstreamDownError) Error() string {
	return "downstream connection error"
}

func (d downstreamDownError) IsRetryable() bool {
	return true
}

func (d downstreamDownError) IsDownStreamDown() bool {
	return true
}

type downstreamFailedWriter struct {
	onWrite writeFunc
}

func (t downstreamFailedWriter) Close(_ context.Context) error {
	return nil
}

func (t downstreamFailedWriter) GracefulShutdown(ctx context.Context) error {
	return nil
}

func (t downstreamFailedWriter) SaveState(ctx context.Context, lsn pglogrepl.LSN) error {
	return nil
}

func (t downstreamFailedWriter) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return nil
}

func (t downstreamFailedWriter) GetSnapshotLSN(ctx context.Context) (string, error) {
	return "", nil
}

func (t downstreamFailedWriter) TxOptions() appconfig.TxCommitTimeOptions {
	return appconfig.TxCommitTimeOptions{}
}

func (t downstreamFailedWriter) Ping(ctx context.Context) error {
	return nil
}

func (t downstreamFailedWriter) Write(_ context.Context, req *pgwal.WriteRequest, tracker ResponseTracker) {
	t.onWrite(req)

	failedEntries := make([]*pgwal.ErrorResponseEntry, len(req.Entries))
	for i, entry := range req.Entries {
		failedEntries[i] = &pgwal.ErrorResponseEntry{
			ResponseEntry: pgwal.ResponseEntry{
				PK:    entry.PK,
				Table: entry.Table,
				LSN:   req.LSN,
			},
			Reason: downstreamDownError{},
		}
	}

	tracker.OnError(&pgwal.ErrResponse{
		Entries: failedEntries,
	})
}

func TestTriggerWithUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer func() {
		cancel()
	}()

	writes := make([]*pgwal.WriteRequest, 0)
	numOfWritesChan := make(chan struct{})
	expectedNumOfWrites := 1

	var onWrite writeFunc = func(req *pgwal.WriteRequest) {
		writes = append(writes, req)
		if len(writes) == expectedNumOfWrites {
			numOfWritesChan <- struct{}{}
		}
	}

	writer := &testPGWALWriter{onWrite: onWrite}
	//logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()
	res, bufListener := mustCreateAndPrepareBufferedListener(t, writer, testpgreplicator.TestPGReplicatorOptions{
		Logger:      logger,
		RetryConfig: circuitbreaker.DefaultRetryConfig(),
		Postgres: testpgreplicator.PGOptions{
			Image:    pgImage,
			User:     userName,
			Password: password,
			CreateTableSQL: `
create table test_tx_tr (id int primary key, data text, version int);
create or replace function test_tx_trigger_proc() returns trigger as $$
begin
    new.version = coalesce(new.version, 0) + 1;
    return new;
end;
$$ language plpgsql;

create trigger test_tx_trigger 
    before update or insert on test_tx_tr 
    for each row 
    execute procedure test_tx_trigger_proc();`,
			TableName: "test_tx_tr",
			SlotName:  "test_slot",
			PubName:   "test_pub",
			NumMode:   0,
		},
	})

	bufListener.Start(ctx)

	repl := replicator.MustCreateWithLogger(replicator.NewOptions(res.Options.Postgres.SlotName,
		res.Options.Postgres.PubName, 5*time.Second, 5*time.Second, 1*time.Hour, 10000),
		bufListener,
		bufListener,
		res.Connector,
		"pg2buffer_test",
		logger)

	logger.Info("starting replicator")

	doneChan := make(chan error)
	err := repl.StartFromLsn(ctx, pglogrepl.LSN(0), doneChan)

	if err != nil {
		t.Fatal("failed to start replicator (#1): " + err.Error())
	}

	err = withTx(ctx, res.Pool, func(ctx context.Context, tx pgx.Tx) error {
		tx.Exec(ctx, "insert into test_tx_tr (id, data, version) values (1, '1', 1)")
		tx.Exec(ctx, "insert into test_tx_tr (id, data, version) values (2, '2', 1)")
		tx.Exec(ctx, "insert into test_tx_tr (id, data, version) values (3, '3', 1)")
		tx.Exec(ctx, "update test_tx_tr set version = 5 where id = 1")
		tx.Exec(ctx, "update test_tx_tr set version = 10 where id = 3")
		tx.Exec(ctx, "update test_tx_tr set version = 15 where id = 2")

		return nil
	})

	timedOut := within(numOfWritesChan)
	if timedOut {
		t.Fatal("timed out waiting for writes to be processed")
	}

	assert.Len(t, writes, expectedNumOfWrites)
	entries := writes[0].Entries
	assert.Len(t, entries, 3)
	assert.Equal(t, pgwal.Insert, entries[0].Kind)
	assert.Equal(t, pgwal.Insert, entries[1].Kind)
	assert.Equal(t, pgwal.Insert, entries[2].Kind)

	assert.Equal(t, "1", entries[0].PK)
	assert.Equal(t, uint32(0), entries[0].Offset)
	assert.Equal(t, int32(6), entries[0].Tuple.GetValue("version"))

	assert.Equal(t, "3", entries[1].PK)
	assert.Equal(t, uint32(1), entries[1].Offset)
	assert.Equal(t, int32(11), entries[1].Tuple.GetValue("version"))

	assert.Equal(t, "2", entries[2].PK)
	assert.Equal(t, uint32(2), entries[2].Offset)
	assert.Equal(t, int32(16), entries[2].Tuple.GetValue("version"))
}

func TestOneRowMultipleWritesInTx(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer func() {
		cancel()
	}()

	writes := make([]*pgwal.WriteRequest, 0)
	numOfWritesChan := make(chan struct{})
	expectedNumOfWrites := 2

	var onWrite writeFunc = func(req *pgwal.WriteRequest) {
		writes = append(writes, req)
		if len(writes) == expectedNumOfWrites {
			numOfWritesChan <- struct{}{}
		}
	}

	writer := &testPGWALWriter{onWrite: onWrite}
	//logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()
	res, bufListener := mustCreateAndPrepareBufferedListener(t, writer, testpgreplicator.TestPGReplicatorOptions{
		Logger:      logger,
		RetryConfig: circuitbreaker.DefaultRetryConfig(),
		Postgres: testpgreplicator.PGOptions{
			Image:          pgImage,
			User:           userName,
			Password:       password,
			CreateTableSQL: "create table test_tx (id int primary key, data text);",
			TableName:      "test_tx",
			SlotName:       "test_slot",
			PubName:        "test_pub",
			NumMode:        0,
		},
	})

	bufListener.Start(ctx)

	repl := replicator.MustCreateWithLogger(replicator.NewOptions(res.Options.Postgres.SlotName,
		res.Options.Postgres.PubName, 5*time.Second, 5*time.Second, 1*time.Hour, 10000),
		bufListener,
		bufListener,
		res.Connector,
		"pg2buffer_test",
		logger)

	logger.Info("starting replicator")

	doneChan := make(chan error)
	err := repl.StartFromLsn(ctx, pglogrepl.LSN(0), doneChan)

	if err != nil {
		t.Fatal("failed to start replicator (#1): " + err.Error())
	}

	err = withTx(ctx, res.Pool, func(ctx context.Context, tx pgx.Tx) error {
		_, err = tx.Exec(ctx, "insert into test_tx (id, data) values (1, '1')")
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, "update test_tx set data = '2' where id = 1")
		if err != nil {
			return err
		}

		return nil
	})

	err = withTx(ctx, res.Pool, func(ctx context.Context, tx pgx.Tx) error {
		_, err = tx.Exec(ctx, "update test_tx set data = '3' where id = 1")
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, "update test_tx set data = '4' where id = 1")
		if err != nil {
			return err
		}

		return nil
	})

	timedOut := within(numOfWritesChan)
	if timedOut {
		t.Fatal("timed out waiting for writes to be processed (#1)")
	}

	assert.Len(t, writes, expectedNumOfWrites)
	// 1st tx - insert with update -> only insert should be generated but data should be from update
	assert.Len(t, writes[0].Entries, 1)
	assert.Equal(t, pgwal.Insert, writes[0].Entries[0].Kind)
	assert.Equal(t, "2", writes[0].Entries[0].Tuple.GetValue("data"))

	// 2nd tx - two updates -> only the latest update should be generated
	assert.Len(t, writes[1].Entries, 1)
	assert.Equal(t, "4", writes[1].Entries[0].Tuple.GetValue("data"))
	assert.Equal(t, pgwal.Update, writes[1].Entries[0].Kind)
}

func withTx(ctx context.Context, pool *pgxpool.Pool, f func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	err = f(ctx, tx)

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func TestDownstreamDownDoNotExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	writes := make([]*pgwal.WriteRequest, 0)
	numOfWritesChan := make(chan struct{})
	expectedNumOfWrites := 6

	var onWrite writeFunc = func(req *pgwal.WriteRequest) {
		writes = append(writes, req)
		if len(writes) == expectedNumOfWrites {
			numOfWritesChan <- struct{}{}
		}
	}

	writer := &downstreamFailedWriter{onWrite: onWrite}
	//logger := zap.NewNop()
	logger, _ := zap.NewDevelopment()

	res, bufListener := mustCreateAndPrepareBufferedListener(t, writer, testpgreplicator.TestPGReplicatorOptions{
		Logger: logger,
		RetryConfig: circuitbreaker.RetryConfig{
			// when MaxConnectionRetries is zero down stream connection errors will be retried indefinitely
			MaxConnectionRetries: 0,
			// we also set MaxRetries to expectedNumOfWrites - 1 to make sure these two configs do not intersect
			MaxRetries: expectedNumOfWrites - 1,

			// small values for quick retries
			InitialBackoff: 1 * time.Millisecond,
			Multiplier:     0.5,
			Jitter:         1.1,
			MaxBackoff:     5 * time.Millisecond,
		},
		Postgres: testpgreplicator.PGOptions{
			Image:          pgImage,
			User:           userName,
			Password:       password,
			CreateTableSQL: "create table downstream_down_test (id int primary key, data text);",
			TableName:      "downstream_down_test",
			SlotName:       "downstream_down_test_slot",
			PubName:        "downstream_down_test_pub",
			NumMode:        0,
		},
	})

	bufListener.Start(ctx)

	repl := replicator.MustCreateWithLogger(replicator.NewOptions(res.Options.Postgres.SlotName,
		res.Options.Postgres.PubName, 5*time.Second, 15*time.Second, 1*time.Hour, 10000),
		bufListener,
		bufListener,
		res.Connector,
		"pg2buffer_downstream_down_test",
		logger)

	logger.Info("starting replicator")

	doneChan := make(chan error)
	err := repl.StartFromLsn(ctx, pglogrepl.LSN(0), doneChan)

	if err != nil {
		t.Fatal("failed to start replicator (#1): " + err.Error())
	}

	res.Pool.Exec(ctx, "insert into downstream_down_test (id, data) values (1, '1')")

	timedOut := within(numOfWritesChan)
	if timedOut {
		t.Fatal("timed out waiting for writes to be processed (#1)")
	}
}

func TestSetupPGReplication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	testOpts := integrationtest.MustCreateReplication(t, ctx, userName, password, pgImage, tableName, slotName, pubName)
	//log, _ := zap.NewDevelopment()
	log := zap.NewNop()

	writes := make([]*pgwal.WriteRequest, 0)
	numOfWritesChan := make(chan struct{})
	expectedNumOfWrites := -1

	var onWrite writeFunc = func(req *pgwal.WriteRequest) {
		writes = append(writes, req)
		if len(writes) == expectedNumOfWrites {
			numOfWritesChan <- struct{}{}
		}
	}

	pgOpts, err := testOpts.OptsMany()
	if err != nil {
		t.Fatal("failed to create PG opts: ", err.Error())
	}

	fmt.Printf("hosts: %s, ports: %s\n", pgOpts.Host, pgOpts.Port)

	pgConnector, err := pgconnector.New(pgOpts, log)
	if err != nil {
		t.Fatal("failed to create pgConnector: " + err.Error())
	}

	_, err = pgConnector.ConnectPrimaryNode(ctx)
	if err != nil {
		t.Fatal("failed to connect to pgConnector: " + err.Error())
	}

	assert.Equal(t, 1, pgConnector.ReplicasCount(), "expected 1 replica")

	writer := &testPGWALWriter{onWrite: onWrite}

	tableNameKey := "public." + tableName

	bufListener := NewListener(writer, ListenerOptions{
		Publication: pubName,
		NumericMode: 0,
	}, pgConnector,
		map[string]*config.TableConfig{
			tableNameKey: &config.TableConfig{
				Columns: []string{"all"},
				Options: make(map[string]string),
			},
		},
		circuitbreaker.DefaultRetryConfig(),
		log)

	err = bufListener.LoadPubTables(ctx)
	if err != nil {
		panic("failed to load pub tables: " + err.Error())
	}

	bufListener.Start(ctx)

	repl := replicator.MustCreateWithLoggerAndBreakerConfig(replicator.NewOptions(slotName, pubName, 5*time.Second, 15*time.Second, 1*time.Hour, 10000),
		bufListener,
		bufListener,
		pgConnector,
		"pg2buffer_test",
		// we need a custom circuit breaker config here to make sure that PGReplicator quits the read loop fast
		// and then tries to reconnect to replica
		circuitbreaker.Config{
			ClosedMaxErrors:     1,
			HalfOpenedMaxErrors: 1,
			MaxErrorsInterval:   time.Second,
			MaxWaitInterval:     time.Second,
		},
		log)

	log.Info("starting replicator on primary")

	doneChan := make(chan error)
	err = repl.StartFromLsn(ctx, pglogrepl.LSN(0), doneChan)

	if err != nil {
		t.Fatal("failed to start replicator (#1): " + err.Error())
	}

	expectedNumOfWrites = 2

	mustInsert(ctx, testOpts.PrimPool, 1, "1")
	mustInsert(ctx, testOpts.PrimPool, 2, "2")

	log.Info("waiting for writes to finish (#1)")

	timedOut := within(numOfWritesChan)
	if timedOut {
		t.Fatal("timed out waiting for writes to be processed (#1)")
	}

	log.Info("stopping primary")

	five := 5 * time.Second
	err = testOpts.Primary.Stop(ctx, &five)

	if err != nil {
		panic("failed to stop primary: " + err.Error())
	}

	// after we stopped the primary, the replicator must reconnect automatically
	//t.Log("waiting for replicator to reconnect")
	//
	//timedOut = within(doneChan)
	//if timedOut {
	//	t.Fatal("timed out waiting for replicator to quit")
	//}

	log.Info("promoting replica")

	_, _, err = testOpts.Replica.Exec(ctx, []string{"pg_ctl", "promote"})

	if err != nil {
		panic("failed to promote replica:" + err.Error())
	}

	log.Info("promoting replica done")

	expectedNumOfWrites = 4

	mustInsert(ctx, testOpts.ReplicaPool, 3, "3")
	mustInsert(ctx, testOpts.ReplicaPool, 4, "4")

	timedOut = within(numOfWritesChan)

	assert.False(t, timedOut, "didn't get expected number of writes within defined interval")
	cancel()
	log.Info("waiting for writes to finish (#2)")
	repl.GracefulShutdown(ctx)

	log.Info("waiting for replicator to quit")

	err = <-doneChan

	require.ErrorIs(t, err, context.Canceled)

	assert.Len(t, writes, 4)
	assert.Equal(t, "1", writes[0].Entries[0].PK)
	assert.Equal(t, "2", writes[1].Entries[0].PK)

	// we shouldn't get previous entries from WAL
	assert.Equal(t, "3", writes[2].Entries[0].PK)
	assert.Equal(t, "4", writes[3].Entries[0].PK)
}

func within[T any](ch chan T) bool {
	tOut := time.After(3000 * time.Second)

	var timedOut bool
	select {
	case <-tOut:
		timedOut = true
	case <-ch:
		timedOut = false
	}

	return timedOut
}

func mustInsert(ctx context.Context, pool *pgxpool.Pool, id int, data string) uint32 {
	row := pool.QueryRow(ctx, fmt.Sprintf("insert into %s (id, data) values ($1, $2) returning txid_current()", tableName), id, data)

	var xid uint32

	err := row.Scan(&xid)

	if err != nil {
		panic("failed to insert #1: " + err.Error())
	}

	return xid
}

type writeFunc func(req *pgwal.WriteRequest)
type testPGWALWriter struct {
	onWrite writeFunc
}

func (t testPGWALWriter) Close(_ context.Context) error {
	return nil
}

func (t testPGWALWriter) GracefulShutdown(ctx context.Context) error {
	return nil
}

func (t testPGWALWriter) SaveState(ctx context.Context, lsn pglogrepl.LSN) error {
	return nil
}

func (t testPGWALWriter) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return nil
}

func (t testPGWALWriter) GetSnapshotLSN(ctx context.Context) (string, error) {
	return "", nil
}

func (t testPGWALWriter) TxOptions() appconfig.TxCommitTimeOptions {
	return appconfig.TxCommitTimeOptions{}
}

func (t testPGWALWriter) Write(_ context.Context, req *pgwal.WriteRequest, tracker ResponseTracker) {
	t.onWrite(req)
	okEntries := make([]*pgwal.ResponseEntry, len(req.Entries))
	for i, entry := range req.Entries {
		okEntries[i] = &pgwal.ResponseEntry{
			PK:    entry.PK,
			Table: entry.Table,
			LSN:   req.LSN,
			XID:   req.XID,
		}
	}
	tracker.OnSuccess(&pgwal.Response{
		Entries: okEntries,
	})
}

func (t testPGWALWriter) Ping(ctx context.Context) error {
	return nil
}
