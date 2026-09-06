package snapshot

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/entrypool"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testSnapshotListener struct {
	mu         sync.Mutex
	batchCh    chan *pgwal.WriteRequest
	batches    []*pgwal.WriteRequest
	tables     map[string]*pgschema.Table
	lsn        string
	commit     pglogrepl2json.CommitPoint
	ackHandler func(*pgwal.Response, pglogrepl2json.CommitPoint)
}

func (l *testSnapshotListener) WriteQueueSize() uint64 {
	l.mu.Lock()
	size := len(l.batches)
	l.mu.Unlock()

	return uint64(size)
}

func (l *testSnapshotListener) TxOptions() appconfig.TxCommitTimeOptions {
	return appconfig.NewTxCommitTimeOptions("")
}

func (l *testSnapshotListener) OnSnapshotBatch(ctx context.Context, table pgschema.TableName, batch *pgwal.WriteRequest) error {
	l.mu.Lock()
	l.batches = append(l.batches, batch)
	handler := l.ackHandler
	batchCh := l.batchCh
	l.mu.Unlock()

	if batchCh != nil {
		select {
		case batchCh <- batch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if handler != nil {
		handler(snapshotResponse(batch))
	}

	l.setCommit(batch.LSN)
	return nil
}

func (l *testSnapshotListener) SetSnapshotAckHandler(handler func(*pgwal.Response, pglogrepl2json.CommitPoint)) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.ackHandler = handler
}

func (l *testSnapshotListener) GetTable(table pgschema.TableName) *pgschema.Table {
	return l.tables[table.FullName]
}

func (l *testSnapshotListener) CommitPos() pglogrepl2json.CommitPoint {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.commit
}

func (l *testSnapshotListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lsn = lsn
	return nil
}

func (l *testSnapshotListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.lsn, nil
}

func (l *testSnapshotListener) GracefulShutdown(ctx context.Context) error {
	return nil
}

func (l *testSnapshotListener) Ack(batch *pgwal.WriteRequest) {
	l.mu.Lock()
	handler := l.ackHandler
	l.mu.Unlock()

	if handler != nil {
		handler(snapshotResponse(batch))
	}

	l.setCommit(batch.LSN)
}

func (l *testSnapshotListener) setCommit(lsn pglogrepl.LSN) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.commit = pglogrepl2json.CommitPoint{LSN: lsn}
}

func snapshotResponse(batch *pgwal.WriteRequest) (*pgwal.Response, pglogrepl2json.CommitPoint) {
	respEntries := make([]*pgwal.ResponseEntry, len(batch.Entries))
	for i, entry := range batch.Entries {
		respEntries[i] = &pgwal.ResponseEntry{
			PK:     entry.PK,
			Table:  entry.Table,
			LSN:    batch.LSN,
			XID:    batch.XID,
			Offset: entry.Offset,
		}
	}

	return &pgwal.Response{Entries: respEntries}, pglogrepl2json.CommitPoint{}
}

func TestSnapshotIntegration(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx := context.Background()
	logger, _ := zap.NewDevelopment()

	// 1. Start Postgres
	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx, "docker.io/postgres:16", "snapshot_test")
	require.NoError(t, err)
	defer pgC.Terminate(ctx)

	pool, err := pgxpool.New(ctx, integrationtest.CreatePGConnStr(config.ConnectionOpts{
		Host:     connOpts.Host,
		Port:     connOpts.Port,
		User:     connOpts.User,
		Password: connOpts.Password,
		Database: connOpts.Database,
	}))
	require.NoError(t, err)
	defer pool.Close()

	// 2. Prepare Data
	_, err = pool.Exec(ctx, `
		CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);
		INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com'), ('Charlie', 'charlie@example.com');
	`)
	require.NoError(t, err)

	// 3. Setup Connector
	connector, err := pgconnector.New(connOpts, logger)
	require.NoError(t, err)
	err = connector.Connect(ctx)
	require.NoError(t, err)
	defer connector.Close(ctx)

	// 4. Setup SnapshotManager
	cfg := &appconfig.SnapshotConfig{
		Mode:      appconfig.ModeOneTime,
		BatchSize: 10,
		Config: []*appconfig.SnapshotTableConfig{
			{
				Name: "public.users",
				Type: appconfig.SnapshotTableFull,
			},
		},
	}

	usersTable := pgschema.NewTable("public", "users")
	usersTable.AddColumn(&pgschema.Column{Name: "id", IsPK: true})
	usersTable.AddColumn(&pgschema.Column{Name: "name"})
	usersTable.AddColumn(&pgschema.Column{Name: "email"})

	listener := &testSnapshotListener{
		tables: map[string]*pgschema.Table{
			"public.users": usersTable,
		},
	}
	manager := NewManager(cfg, connector, listener, "test-app", logger)

	// 5. Run Snapshot
	lsn, err := manager.Execute(ctx)
	require.NoError(t, err)
	assert.NotZero(t, lsn)

	// 6. Verify Results
	assert.Equal(t, 1, len(listener.batches))
	assert.Equal(t, 3, len(listener.batches[0].Entries))

	expectedRows := []struct {
		pk    string
		id    int32
		name  string
		email string
	}{
		{pk: "1", id: 1, name: "Alice", email: "alice@example.com"},
		{pk: "2", id: 2, name: "Bob", email: "bob@example.com"},
		{pk: "3", id: 3, name: "Charlie", email: "charlie@example.com"},
	}

	for i, entry := range listener.batches[0].Entries {
		require.NotNil(t, entry)
		assert.Equal(t, pgwal.SnapshotRead, entry.Kind)
		assert.Equal(t, "public.users", entry.Table.FullName)
		assert.Equal(t, expectedRows[i].pk, entry.PK)
		require.NotNil(t, entry.Tuple)

		idValue, ok := entry.Tuple.Get("id")
		require.True(t, ok)
		assert.Equal(t, expectedRows[i].id, idValue)

		nameValue, ok := entry.Tuple.Get("name")
		require.True(t, ok)
		assert.Equal(t, expectedRows[i].name, nameValue)

		emailValue, ok := entry.Tuple.Get("email")
		require.True(t, ok)
		assert.Equal(t, expectedRows[i].email, emailValue)
	}

	// Check state in DB
	var status string
	var processed int64
	var snapshotStart time.Time
	var snapshotEnd time.Time
	err = pool.QueryRow(ctx, "SELECT status, processed_rows, snapshot_start, snapshot_end FROM pgwalk.snapshot_state WHERE table_name = 'public.users'").Scan(&status, &processed, &snapshotStart, &snapshotEnd)
	require.NoError(t, err)
	assert.Equal(t, "completed", status)
	assert.Equal(t, int64(3), processed)
	assert.False(t, snapshotEnd.Before(snapshotStart))
}

func TestSnapshotIntegrationUsesPublicationColumns(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx, "docker.io/postgres:16", "snapshot_publication_columns_test")
	require.NoError(t, err)
	defer pgC.Terminate(ctx)

	pool, err := pgxpool.New(ctx, integrationtest.CreatePGConnStr(config.ConnectionOpts{
		Host:     connOpts.Host,
		Port:     connOpts.Port,
		User:     connOpts.User,
		Password: connOpts.Password,
		Database: connOpts.Database,
	}))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE full_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
		CREATE TABLE partial_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
		INSERT INTO full_users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
		INSERT INTO partial_users (id, name, email) VALUES (2, 'Bob', 'bob@example.com');
		CREATE PUBLICATION snapshot_publication_columns
			FOR TABLE full_users, partial_users (id, name);
	`)
	require.NoError(t, err)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	pubRes, err := pgschema.ReadPubTables(ctx, conn.Conn(), "snapshot_publication_columns", pgschema.V16)
	conn.Release()
	require.NoError(t, err)
	assert.True(t, pubRes.Tables["public.full_users"].HasAllPhysicalColumns())
	assert.False(t, pubRes.Tables["public.partial_users"].HasAllPhysicalColumns())

	connector, err := pgconnector.New(connOpts, logger)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(ctx))
	defer connector.Close(ctx)

	listener := &testSnapshotListener{
		tables: pubRes.Tables,
	}
	manager := NewManager(&appconfig.SnapshotConfig{
		Mode:      appconfig.ModeOneTime,
		BatchSize: 10,
		Config: []*appconfig.SnapshotTableConfig{
			{
				Name: "public.full_users",
				Type: appconfig.SnapshotTableFull,
			},
			{
				Name: "public.partial_users",
				Type: appconfig.SnapshotTableFull,
			},
		},
	}, connector, listener, "test-app", logger)

	_, err = manager.Execute(ctx)
	require.NoError(t, err)

	var fullQuery string
	err = pool.QueryRow(ctx, "SELECT snapshot_query FROM pgwalk.snapshot_state WHERE table_name = 'public.full_users'").Scan(&fullQuery)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM public.full_users", fullQuery)

	var partialQuery string
	err = pool.QueryRow(ctx, "SELECT snapshot_query FROM pgwalk.snapshot_state WHERE table_name = 'public.partial_users'").Scan(&partialQuery)
	require.NoError(t, err)
	assert.Equal(t, "SELECT id, name FROM public.partial_users", partialQuery)

	entries := make(map[string]*pgwal.WriteEntry)
	for _, batch := range listener.batches {
		for _, entry := range batch.Entries {
			entries[entry.Table.FullName] = entry
		}
	}

	require.Contains(t, entries, "public.full_users")
	assert.Equal(t, uint32(3), entries["public.full_users"].Tuple.Size())
	_, ok := entries["public.full_users"].Tuple.Get("email")
	assert.True(t, ok)

	require.Contains(t, entries, "public.partial_users")
	assert.Equal(t, uint32(2), entries["public.partial_users"].Tuple.Size())
	_, ok = entries["public.partial_users"].Tuple.Get("email")
	assert.False(t, ok)
}

func TestSnapshotIntegrationEmptyTableCompletesWithoutBatches(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx, "docker.io/postgres:16", "snapshot_empty_table_test")
	require.NoError(t, err)
	defer pgC.Terminate(ctx)

	pool, err := pgxpool.New(ctx, integrationtest.CreatePGConnStr(config.ConnectionOpts{
		Host:     connOpts.Host,
		Port:     connOpts.Port,
		User:     connOpts.User,
		Password: connOpts.Password,
		Database: connOpts.Database,
	}))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `CREATE TABLE empty_users (id INTEGER PRIMARY KEY, name TEXT);`)
	require.NoError(t, err)

	connector, err := pgconnector.New(connOpts, logger)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(ctx))
	defer connector.Close(ctx)

	emptyUsersTable := pgschema.NewTable("public", "empty_users")
	emptyUsersTable.AddColumn(&pgschema.Column{Name: "id", IsPK: true, Position: 1})
	emptyUsersTable.AddColumn(&pgschema.Column{Name: "name", Position: 2})

	listener := &testSnapshotListener{
		tables: map[string]*pgschema.Table{
			"public.empty_users": emptyUsersTable,
		},
	}
	manager := NewManager(&appconfig.SnapshotConfig{
		Mode:      appconfig.ModeOneTime,
		BatchSize: 10,
		Config: []*appconfig.SnapshotTableConfig{
			{
				Name: "public.empty_users",
				Type: appconfig.SnapshotTableFull,
			},
		},
	}, connector, listener, "test-app", logger)

	lsn, err := manager.Execute(ctx)
	require.NoError(t, err)
	assert.NotZero(t, lsn)
	assert.Empty(t, listener.batches)

	var status string
	var processed int64
	var snapshotStart time.Time
	var snapshotEnd time.Time
	err = pool.QueryRow(ctx, "SELECT status, processed_rows, snapshot_start, snapshot_end FROM pgwalk.snapshot_state WHERE table_name = 'public.empty_users'").Scan(&status, &processed, &snapshotStart, &snapshotEnd)
	require.NoError(t, err)
	assert.Equal(t, "completed", status)
	assert.Equal(t, int64(0), processed)
	assert.False(t, snapshotEnd.Before(snapshotStart))
}

func TestSnapshotProgressUpdatesAfterAcknowledgement(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	logger := zap.NewNop()

	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx, "docker.io/postgres:16", "snapshot_ack_progress_test")
	require.NoError(t, err)
	defer pgC.Terminate(context.Background())

	pool, err := pgxpool.New(ctx, integrationtest.CreatePGConnStr(config.ConnectionOpts{
		Host:     connOpts.Host,
		Port:     connOpts.Port,
		User:     connOpts.User,
		Password: connOpts.Password,
		Database: connOpts.Database,
	}))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
	`)
	require.NoError(t, err)

	connector, err := pgconnector.New(connOpts, logger)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(ctx))
	defer connector.Close(context.Background())

	usersTable := pgschema.NewTable("public", "users")
	usersTable.AddColumn(&pgschema.Column{Name: "id", IsPK: true})
	usersTable.AddColumn(&pgschema.Column{Name: "name"})

	listener := &testSnapshotListener{
		batchCh: make(chan *pgwal.WriteRequest, 1),
		tables: map[string]*pgschema.Table{
			"public.users": usersTable,
		},
	}
	manager := NewManager(&appconfig.SnapshotConfig{
		Mode:            appconfig.ModeOneTime,
		BatchSize:       10,
		ParallelWorkers: 1,
		Config: []*appconfig.SnapshotTableConfig{
			{
				Name: "public.users",
				Type: appconfig.SnapshotTableFull,
			},
		},
	}, connector, listener, "test-app", logger)

	errCh := make(chan error, 1)
	go func() {
		_, err := manager.Execute(ctx)
		errCh <- err
	}()

	var batch *pgwal.WriteRequest
	select {
	case batch = <-listener.batchCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for snapshot batch dispatch")
	}

	var status string
	var processed int64
	err = pool.QueryRow(ctx, "SELECT status, processed_rows FROM pgwalk.snapshot_state WHERE table_name = 'public.users'").Scan(&status, &processed)
	require.NoError(t, err)
	assert.Equal(t, "in_progress", status)
	assert.Equal(t, int64(0), processed)

	listener.Ack(batch)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for snapshot completion")
	}

	err = pool.QueryRow(ctx, "SELECT status, processed_rows FROM pgwalk.snapshot_state WHERE table_name = 'public.users'").Scan(&status, &processed)
	require.NoError(t, err)
	assert.Equal(t, "completed", status)
	assert.Equal(t, int64(3), processed)
}

func TestWorkerProcessTableRangeDispatchesSnapshotBatches(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx, "docker.io/postgres:16", "snapshot_deadlock_test")
	require.NoError(t, err)
	defer pgC.Terminate(ctx)

	pool, err := pgxpool.New(ctx, integrationtest.CreatePGConnStr(config.ConnectionOpts{
		Host:     connOpts.Host,
		Port:     connOpts.Port,
		User:     connOpts.User,
		Password: connOpts.Password,
		Database: connOpts.Database,
	}))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO users (id, name)
		SELECT i, 'user-' || i FROM generate_series(1, 3) AS i;
	`)
	require.NoError(t, err)

	connector, err := pgconnector.New(connOpts, logger)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(ctx))
	defer connector.Close(ctx)

	usersTable := pgschema.NewTable("public", "users")
	usersTable.AddColumn(&pgschema.Column{Name: "id", IsPK: true, Position: 1})
	usersTable.AddColumn(&pgschema.Column{Name: "name", Position: 2})

	listener := &testSnapshotListener{
		tables: map[string]*pgschema.Table{
			"public.users": usersTable,
		},
	}

	workerCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := NewWorker(1, connector, listener, 1, "", &snapshotLSNTracker{}, appconfig.NewTxCommitTimeOptions(""),
		entrypool.NewPool(),
		logger)
	progressReporter := newSnapshotProgressReporterAndDispatch(10, logger)
	err = worker.ProcessTableRange(workerCtx, usersTable, progressReporter, "SELECT id, name FROM public.users ORDER BY id")
	require.NoError(t, err)
	assert.Len(t, listener.batches, 3)
}

func TestWorkerProcessReusesConnectionAcrossTasks(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx := context.Background()
	logger := zap.NewNop()

	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx, "docker.io/postgres:16", "snapshot_worker_reuse_test")
	require.NoError(t, err)
	defer pgC.Terminate(ctx)

	pool, err := pgxpool.New(ctx, integrationtest.CreatePGConnStr(config.ConnectionOpts{
		Host:     connOpts.Host,
		Port:     connOpts.Port,
		User:     connOpts.User,
		Password: connOpts.Password,
		Database: connOpts.Database,
	}))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
	`)
	require.NoError(t, err)

	connector, err := pgconnector.New(connOpts, logger)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(ctx))
	defer connector.Close(ctx)

	usersTable := pgschema.NewTable("public", "users")
	usersTable.AddColumn(&pgschema.Column{Name: "id", IsPK: true, Position: 1})
	usersTable.AddColumn(&pgschema.Column{Name: "name", Position: 2})

	listener := &testSnapshotListener{
		tables: map[string]*pgschema.Table{
			"public.users": usersTable,
		},
	}

	taskCh := make(chan snapshotTask, 2)
	taskCh <- snapshotTask{
		state: &TableState{TableName: "public.users"},
		table: usersTable,
		query: "SELECT id, name, pg_backend_pid() AS worker_pid FROM public.users WHERE id = 1",
	}
	taskCh <- snapshotTask{
		state: &TableState{TableName: "public.users"},
		table: usersTable,
		query: "SELECT id, name, pg_backend_pid() AS worker_pid FROM public.users WHERE id = 2",
	}
	close(taskCh)

	errChan := make(chan tableError, 1)
	progressReporter := newSnapshotProgressReporterAndDispatch(10, logger)
	defer func() {
		progressReporter.Close()
		for range progressReporter.Reports() {
		}
	}()

	workerCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	worker := NewWorker(1, connector, listener, 1, "", &snapshotLSNTracker{}, appconfig.NewTxCommitTimeOptions(""), entrypool.NewPool(), logger)
	worker.Process(workerCtx, 1, taskCh, errChan, progressReporter)

	select {
	case workerErr := <-errChan:
		t.Fatalf("worker failed unexpectedly: %v", workerErr.err)
	default:
	}

	require.Len(t, listener.batches, 2)
	require.Len(t, listener.batches[0].Entries, 1)
	require.Len(t, listener.batches[1].Entries, 1)

	firstPID, ok := listener.batches[0].Entries[0].Tuple.Get("worker_pid")
	require.True(t, ok)
	secondPID, ok := listener.batches[1].Entries[0].Tuple.Get("worker_pid")
	require.True(t, ok)
	assert.Equal(t, firstPID, secondPID)
}
