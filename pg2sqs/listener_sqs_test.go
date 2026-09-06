package pg2sqs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsconfig"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsops"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgCont "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
)

// TODO:
// 1. Deletes should not be sent -> CANCELLED (see 2)
// 2. Make a config which allows to send to different queues based on kind/tablename -> DONE
// 3. Test that some entries are ignored -> DONE
// 4. Batch send (max count or timeout) -> DONE
// 5. On shutdown all inflight requests should be finished and progress reported back to postgres
// 6. Make sure CommitPoint is reported in order -> DONE
// 7. DedupID, GroupID
// 8. Check all TODO:
// 9. Check migrator: it always sends ALTER PUBLICATION when config has columns: all -> DONE
// 10. BUG: start pg2sqs, update some entries and make sure that confirmed_flush_lsn is updated. stop p2sqs.
// 10.1 start pg2sqs, run no transactions, stop pg2sqs -> confirmed_flush_lsn is set to 0/1
// 10.2 start pg2sqs -> old transactions are delivered again.
// 10.x fix howto: we probably need to load confirmed_flush_lsn from the database -> DONE
// 11. Check all panic()

var (
	enableLog bool
)

func TestMain(m *testing.M) {
	flag.BoolVar(&enableLog, "log", false, "enable log")

	// manually call flag.Parse so that testing.Short() won't panic
	flag.Parse()

	// docker run --name postgres-14 -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres:14
	var (
		pgImages []string
		sqsImage = "softwaremill/elasticmq:latest"
	)

	if testing.Short() {
		pgImages = []string{
			"docker.io/postgres:16",
		}
	} else {
		pgImages = integrationtest.PGImages
	}

	for _, pgImage := range pgImages {
		fmt.Println("----- TEST START -----")
		fmt.Printf("PG: %v, SQS: %v\n", pgImage, sqsImage)

		runMainWithContainers(m, pgImage, sqsImage)
		fmt.Println("----- TEST END   ----- ")
	}
}

func runMainWithContainers(m *testing.M, pgImage, sqsImage string) {
	ctx := context.Background()

	initScripts := []string{filepath.Join("../internal/integrationtest/testdata", "initpg.sql")}
	if !strings.Contains(pgImage, "postgres:12") {
		initScripts = append(initScripts, filepath.Join("../internal/integrationtest/testdata", "initpgv13.sql"))
	}

	pgC, err := integrationtest.CreatePGContainer(ctx, pgImage, sqsconfig.Prefix(), pgCont.WithInitScripts(initScripts...))
	if err != nil {
		panic(fmt.Errorf("failed to start postgres container: %v", err))
	}

	sqsReq := testcontainers.ContainerRequest{
		Image:        sqsImage,
		ExposedPorts: []string{"9324/tcp"},
		WaitingFor:   wait.NewLogStrategy("=== ElasticMQ server .+ started in \\d+ ms ===").AsRegexp(),
	}

	sqsC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: sqsReq,
		Started:          true,
	})

	if err != nil {
		panic(fmt.Errorf("failed to start SQS container: %v", err))
	}

	defer func() {
		sqsC.Terminate(ctx) //nolint
		pgC.Terminate(ctx)  //nolint
	}()

	var (
		p                   string
		attempt, maxAttempt = 0, 3
	)

	for attempt < maxAttempt {
		p, err = sqsC.Endpoint(ctx, "")
		if err == nil {
			break
		}

		attempt++
	}

	if err != nil {
		fmt.Printf("SQS endpoint error: %+v\n", err)
		panic(err)
	}

	fmt.Printf("SQS endpoint: %+v\n", p)
	hostPort := strings.Split(p, ":")
	os.Setenv("AWS_ENDPOINT_URL_SQS", fmt.Sprintf("http://%s:%s", hostPort[0], hostPort[1]))

	runMain(m)
}

func runMain(m *testing.M) {
	appConf, err := sqsconfig.LoadAppConfig("")
	if err != nil {
		panic("failed to load app config: " + err.Error())
	}

	integrationtest.MustLogicalDecodingWorkMemWithConfig(appConf.Postgres)
	m.Run()
}

func mustLoadAppConfig(t *testing.T, configPath ...string) *sqsconfig.SQSAppConfig {
	appConf, err := sqsconfig.LoadAppConfig(configPath...)

	require.NoError(t, err)

	val := t.Context().Value("FlushInterval")
	if val != nil && val.(uint32) > 0 {
		appConf.SQSFlushInterval = fmt.Sprintf("%dms", val.(uint32))
	} else {
		appConf.SQSFlushInterval = "50ms" // ms
	}

	pgConfig := &appConf.Postgres
	wellFormed := strings.ToLower(strings.Replace(t.Name(), "/", "", -1))

	pgConfig.Repl.Slot = wellFormed + "_slot"
	pgConfig.Repl.Pub = wellFormed + "_pub"

	return appConf
}

type sqsTest struct {
	appConfig      *sqsconfig.SQSAppConfig
	listenerConfig *SQSListenerOptions
	sqsClient      *sqs.Client
	pool           *pgxpool.Pool
	log            *zap.Logger
	connector      *pgconnector.PGConnector
	testCtx        *TestContext
}

type TestContext struct {
	pgTestOpts      integrationtest.PGTestOptions
	pgMultiTestOpts integrationtest.PGTestMultiOptions
	listener        *SQSListener
	runCtx          context.Context
	t               *testing.T
}

type TestTable struct {
	ID        int    `json:"id"`
	FieldInt  int    `json:"field_int"`
	FieldData []byte `json:"field_data"`
}

func (s *sqsTest) mustInsert(entries ...*TestTable) []uint32 {
	tx, err := s.pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		panic(fmt.Errorf("failed to begin transaction: %v", err))
	}

	defer tx.Rollback(context.Background())

	ids := make([]uint32, len(entries))

	for i, tt := range entries {
		r, err := tx.Query(context.Background(),
			fmt.Sprintf("INSERT INTO%s%s (id, field_int, field_data) VALUES ($1, $2, $3) RETURNING txid_current()", " ", s.testCtx.pgTestOpts.Table),
			tt.ID,
			tt.FieldInt,
			tt.FieldData)

		if err != nil {
			panic(fmt.Errorf("failed to insert into %s", s.testCtx.pgTestOpts.Table))
		}

		var xid uint32

		if !r.Next() {
			r.Close()
			panic(fmt.Errorf("insert didn't return current xid"))
		}

		err = r.Scan(&xid)
		if err != nil {
			r.Close()
			panic("failed to scan xid after insert")
		}

		r.Close()

		ids[i] = xid
	}

	err = tx.Commit(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to commit transaction: %v", err))
	}

	return ids
}

func (s *sqsTest) mustUpdate(entries ...*TestTable) []uint32 {
	tx, err := s.pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		panic(fmt.Errorf("failed to begin transaction: %v", err))
	}

	defer tx.Rollback(context.Background())

	ids := make([]uint32, len(entries))

	for i, tt := range entries {
		r, err := tx.Query(context.Background(),
			fmt.Sprintf("UPDATE%s%s SET field_int=$1, field_data=$2 WHERE id=$3 RETURNING txid_current()", " ", s.testCtx.pgTestOpts.Table),
			tt.FieldInt,
			tt.FieldData,
			tt.ID)

		if err != nil {
			panic(fmt.Errorf("failed to update %s", s.testCtx.pgTestOpts.Table))
		}

		var xid uint32

		if !r.Next() {
			r.Close()
			panic(fmt.Errorf("update didn't return current xid"))
		}

		err = r.Scan(&xid)
		if err != nil {
			r.Close()
			panic("failed to scan xid after update")
		}

		r.Close()

		ids[i] = xid
	}

	err = tx.Commit(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to commit transaction: %v", err))
	}

	return ids
}

func (s *sqsTest) mustDelete(t *testing.T, id int) {
	tx, err := s.pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		panic(fmt.Errorf("failed to begin transaction: %v", err))
	}

	defer tx.Rollback(context.Background()) //nolint
	_, err = tx.Exec(t.Context(), fmt.Sprintf("DELETE FROM%s%s where id = $1", " ", s.testCtx.pgTestOpts.Table), id)
	require.NoError(t, err)
	tx.Commit(t.Context()) //nolint
}

type SQSTestSetup interface {
	integrationtest.TableNameProvider

	UpdateConfig(appConfig *sqsconfig.SQSAppConfig)
}

func (s *sqsTest) MustSetup(t *testing.T, test SQSTestSetup) {
	s.appConfig = mustLoadAppConfig(t)
	lc, _ := NewListenerConfig(s.appConfig)
	lc.ListenerOpts.Publication = s.appConfig.Postgres.Repl.Pub

	s.listenerConfig = lc

	awsCfg, err := awsconfig.LoadDefaultConfig(t.Context(), awsconfig.WithCredentialsProvider(aws.AnonymousCredentials{}))

	require.NoError(t, err)

	sqsClient := sqs.NewFromConfig(awsCfg)
	s.sqsClient = sqsClient

	connStr := s.appConfig.Postgres.Conn.AsOpt().CreateConnStr()
	pgPool, err := pgxpool.New(t.Context(), connStr)

	require.NoError(t, err)

	t.Cleanup(func() {
		pgPool.Close()
	})

	s.pool = pgPool

	testOpts := integrationtest.MustCreateMultiTestOptionsWithConfig(t.Context(), s.appConfig.Postgres, test)

	var log *zap.Logger
	if enableLog {
		log, _ = zap.NewDevelopment()
	} else {
		log = zap.NewNop()
	}

	s.log = log

	if s.appConfig.Tables == nil {
		s.appConfig.Tables = make(map[string]*sqsconfig.SQSTableConfig)
	}

	if len(s.appConfig.Tables) == 0 {
		for _, table := range testOpts.Tables {
			s.appConfig.Tables[fmt.Sprintf("public.%s", table)] = &sqsconfig.SQSTableConfig{
				Columns: []string{"all"},
			}
		}
	}

	// should update queue config for each table
	test.UpdateConfig(s.appConfig)

	connector, err := pgconnector.New(s.appConfig.Postgres.Conn, log)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	t.Cleanup(func() {
		connector.Close(context.Background()) //nolint
	})

	s.connector = connector

	l := NewListener(sqsClient, lc, connector, s.appConfig, log)
	err = l.LoadPubTables(context.Background())
	require.NoError(t, err)

	// this creates a default queue for the default table
	err = l.Ping(context.Background())
	require.NoError(t, err)

	s.testCtx = &TestContext{
		pgTestOpts: integrationtest.PGTestOptions{
			Table:   "",
			Slot:    testOpts.Slot,
			Pub:     testOpts.Pub,
			Timeout: testOpts.Timeout,
			Opts:    testOpts.Opts,
		},
		pgMultiTestOpts: testOpts,
		runCtx:          t.Context(),
		t:               t,
		listener:        l,
	}
}

func (s *sqsTest) Setup(t *testing.T, sqsQueueConfig *sqsconfig.SQSQueueConfig) {
	s.appConfig = mustLoadAppConfig(t)

	lc, _ := NewListenerConfig(s.appConfig)
	lc.ListenerOpts.Publication = s.appConfig.Postgres.Repl.Pub

	s.listenerConfig = lc

	awsCfg, err := awsconfig.LoadDefaultConfig(t.Context(), awsconfig.WithCredentialsProvider(aws.AnonymousCredentials{}))

	require.NoError(t, err)

	//sqsClient := sqs.NewFromConfig(awsCfg, func(options *sqs.Options) {
	//	if s.appConfig.SQS.BaseEndpoint != "" {
	//		options.BaseEndpoint = aws.String(s.appConfig.SQS.BaseEndpoint)
	//	}
	//})
	sqsClient := sqs.NewFromConfig(awsCfg)
	s.sqsClient = sqsClient

	connStr := s.appConfig.Postgres.Conn.AsOpt().CreateConnStr()
	pgPool, err := pgxpool.New(t.Context(), connStr)

	require.NoError(t, err)

	t.Cleanup(func() {
		pgPool.Close()
	})

	s.pool = pgPool

	testOpts := integrationtest.MustCreateTestOptionsWithConfig(t.Context(), s.appConfig.Postgres)

	if s.appConfig.Tables == nil {
		s.appConfig.Tables = make(map[string]*sqsconfig.SQSTableConfig)
	}
	s.appConfig.Tables[fmt.Sprintf("public.%s", testOpts.Table)] = &sqsconfig.SQSTableConfig{
		Columns:     []string{config.AllColumns},
		Options:     nil,
		QueueConfig: sqsQueueConfig,
	}

	var log *zap.Logger
	if enableLog {
		log, _ = zap.NewDevelopment()
	} else {
		log = zap.NewNop()
	}

	s.log = log

	connector, err := pgconnector.New(s.appConfig.Postgres.Conn, log)

	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	t.Cleanup(func() {
		connector.Close(context.Background()) //nolint
	})

	s.connector = connector

	l := NewListener(sqsClient, lc, connector, s.appConfig, log)
	err = l.LoadPubTables(context.Background())
	require.NoError(t, err)

	// this creates a default queue for the default table
	err = l.Ping(context.Background())
	require.NoError(t, err)

	s.testCtx = &TestContext{
		pgTestOpts: testOpts,
		runCtx:     t.Context(),
		t:          t,
		listener:   l,
	}
}

func (s *sqsTest) TearDown() {
	defer s.pool.Close()

	if s.testCtx.pgTestOpts.Table != "" {
		_, err := s.pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE%s%s", " ", s.testCtx.pgTestOpts.Table))
		if err != nil {
			panic(err)
		}
	}

	if len(s.testCtx.pgMultiTestOpts.Tables) > 0 {
		for _, table := range s.testCtx.pgMultiTestOpts.Tables {
			_, err := s.pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE%s%s", " ", table))
			if err != nil {
				panic(err)
			}
		}
	}
}

type TestRunner interface {
	act()
	assert(t *testing.T)
}

type TestQueueReader interface {
	QueueNames() []string
}

func TestSendMessageToSQS(t *testing.T) {
	test := &SendMessageToSQSTest{
		queueName: t.Name() + "_queue",
	}
	test.Setup(t, &sqsconfig.SQSQueueConfig{
		Name: test.queueName,
	})

	defer test.TearDown()

	test.runTest(t, test)
}

type SendMessageToSQSTest struct {
	sqsTest
	tt        *TestTable
	queueName string
}

func (s *SendMessageToSQSTest) act() {
	tt := &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}
	s.mustInsert(tt)
	s.tt = tt
}

func (s *SendMessageToSQSTest) QueueNames() []string {
	return []string{s.queueName}
}

func (s *SendMessageToSQSTest) assert(t *testing.T) {
	m := s.receiveMessages(t, s, 5*time.Second)

	require.Len(t, m, 1)
	messages := m[s.queueName]

	require.Len(t, messages, 1)
	msgBody := messages[0].Body

	var x TestTable

	err := json.Unmarshal([]byte(*msgBody), &x)

	require.NoError(t, err)
	assert.Equal(t, s.tt, &x)
}

func TestManyEntriesInOneTx(t *testing.T) {
	test := &ManyEntriesInOneTxTest{ //nolint
		queueName: t.Name() + "_queue",
	}
	test.Setup(t, &sqsconfig.SQSQueueConfig{ //nolint
		Name: test.queueName,
	})

	defer test.TearDown()
	test.runTest(t, test)
}

type ManyEntriesInOneTxTest struct {
	sqsTest
	tt        []*TestTable
	queueName string
}

func (s *ManyEntriesInOneTxTest) QueueNames() []string {
	return []string{s.queueName}
}

func (s *ManyEntriesInOneTxTest) act() {
	const entriesCount = 5
	tt := make([]*TestTable, entriesCount)

	for i := 0; i < entriesCount; i++ {
		t := &TestTable{
			ID:        i,
			FieldInt:  i,
			FieldData: []byte{byte(i), byte(i + 1)},
		}
		tt[i] = t
	}

	s.tt = tt

	s.mustInsert(tt...)
}

func (s *ManyEntriesInOneTxTest) assert(t *testing.T) {
	allMsg := s.receiveMessages(t, s, 5*time.Second)
	require.Len(t, allMsg, 1)

	messages := allMsg[s.queueName]
	require.Len(t, messages, len(s.tt))

	for _, msg := range messages {
		var ttSQS TestTable
		err := json.Unmarshal([]byte(*msg.Body), &ttSQS)
		require.NoError(t, err)

		ttInserted := s.findAndRemoveByID(ttSQS.ID)
		require.NotNil(t, ttInserted)
		assert.Equal(t, ttInserted, &ttSQS)
	}
}

func (s *ManyEntriesInOneTxTest) findAndRemoveByID(id int) *TestTable {
	if len(s.tt) == 0 {
		return nil
	}

	var i int = -1

	for i = 0; i < len(s.tt); i++ {
		if s.tt[i].ID == id {
			break
		}
	}

	if i == -1 || i == len(s.tt) {
		return nil
	}

	tt := s.tt[i]

	s.tt[i] = s.tt[len(s.tt)-1]
	s.tt = s.tt[:len(s.tt)-1]

	return tt
}

func TestAllSentToUniqueQueue(t *testing.T) {
	test := &SQSAllQueuesTest{
		insertQueueName: t.Name() + "_queue_insert",
		updateQueueName: t.Name() + "_queue_update",
		deleteQueueName: t.Name() + "_queue_delete",
	}
	test.Setup(t, &sqsconfig.SQSQueueConfig{
		Insert: &sqsconfig.SQSQueueConfig{
			Name: test.insertQueueName,
		},
		Update: &sqsconfig.SQSQueueConfig{
			Name: test.updateQueueName,
		},
		Delete: &sqsconfig.SQSQueueConfig{
			Name: test.deleteQueueName,
		},
	})

	defer test.TearDown()
	test.runTest(t, test)
}

type SQSAllQueuesTest struct {
	sqsTest
	insertTT        *TestTable
	updateTT        *TestTable
	insertQueueName string
	updateQueueName string
	deleteQueueName string
}

func (s *SQSAllQueuesTest) QueueNames() []string {
	return []string{s.insertQueueName, s.updateQueueName, s.deleteQueueName}
}

func (s *SQSAllQueuesTest) act() {
	s.insertTT = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}
	s.updateTT = &TestTable{
		ID:        1,
		FieldInt:  3,
		FieldData: []byte{4, 5, 6},
	}
	s.mustInsert(s.insertTT)
	s.mustUpdate(s.updateTT)
	s.mustDelete(s.testCtx.t, s.insertTT.ID)
}

func (s *SQSAllQueuesTest) assert(t *testing.T) {
	messages := s.receiveMessages(t, s, 5*time.Second)
	require.Len(t, messages, 3) // insert, update, delete

	var ttSQS TestTable

	insertMsg, exists := messages[s.insertQueueName]

	require.True(t, exists)
	require.Len(t, insertMsg, 1)
	err := json.Unmarshal([]byte(*insertMsg[0].Body), &ttSQS)
	require.NoError(t, err)
	assert.Equal(t, s.insertTT, &ttSQS)

	updateMsg, exists := messages[s.updateQueueName]

	assert.True(t, exists)
	require.Len(t, updateMsg, 1)
	err = json.Unmarshal([]byte(*updateMsg[0].Body), &ttSQS)
	require.NoError(t, err)
	assert.Equal(t, s.updateTT, &ttSQS)

	deleteMsg, exists := messages[s.deleteQueueName]

	assert.True(t, exists)
	require.Len(t, deleteMsg, 1)
	err = json.Unmarshal([]byte(*deleteMsg[0].Body), &ttSQS)
	require.NoError(t, err)
	assert.Equal(t, s.updateTT, &ttSQS)
}

func TestAllOpsSameQueue(t *testing.T) {
	test := &SQSAllOpsSameQueueTest{
		queueName: t.Name() + "_queue",
	}
	test.Setup(t, &sqsconfig.SQSQueueConfig{
		Name: test.queueName,
	})

	defer test.TearDown()

	test.runTest(t, test)
}

type SQSAllOpsSameQueueTest struct {
	sqsTest
	insertTT  *TestTable
	updateTT  *TestTable
	queueName string
}

func (s *SQSAllOpsSameQueueTest) QueueNames() []string {
	return []string{s.queueName}
}

func (s *SQSAllOpsSameQueueTest) act() {
	s.insertTT = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}
	s.updateTT = &TestTable{
		ID:        1,
		FieldInt:  3,
		FieldData: []byte{4, 5, 6},
	}
	s.mustInsert(s.insertTT)
	s.mustUpdate(s.updateTT)
	s.mustDelete(s.testCtx.t, s.insertTT.ID)
}

func (s *SQSAllOpsSameQueueTest) assert(t *testing.T) {
	messages := s.receiveMessages(t, s, 5*time.Second)
	require.Len(t, messages, 1)

	queueMessages, exists := messages[s.queueName]
	require.True(t, exists)
	require.Len(t, queueMessages, 3) // insert, update, delete

	queueMaps := make([]map[string]any, 0)

	for _, message := range queueMessages {
		var m map[string]any
		err := json.Unmarshal([]byte(*message.Body), &m)
		require.NoError(t, err)

		queueMaps = append(queueMaps, m)
	}

	findAndRemove := func(kind string) map[string]any {
		foundIndex := -1

		for i, m := range queueMaps {
			if m["kind"] == kind {
				foundIndex = i
				break
			}
		}

		require.NotEqual(t, -1, foundIndex)
		result := queueMaps[foundIndex]
		queueMaps[foundIndex] = queueMaps[len(queueMaps)-1]
		queueMaps = queueMaps[:len(queueMaps)-1]

		return result
	}

	m := findAndRemove("insert")
	assert.Equal(t, float64(s.insertTT.ID), m["id"])
	assert.Equal(t, float64(s.insertTT.FieldInt), m["field_int"])
	assert.Equal(t, "insert", m["kind"])

	m = findAndRemove("update")
	assert.Equal(t, float64(s.updateTT.ID), m["id"])
	assert.Equal(t, float64(s.updateTT.FieldInt), m["field_int"])
	assert.Equal(t, "update", m["kind"])

	m = findAndRemove("delete")
	assert.Equal(t, float64(s.updateTT.ID), m["id"])
	assert.Equal(t, float64(s.updateTT.FieldInt), m["field_int"])
	assert.Equal(t, "delete", m["kind"])
}

type DedupGroupIDTest struct {
	sqsTest
	tt        *TestTable
	xid       uint32
	queueName string
}

func TestDedupGroupID(t *testing.T) {
	test := &DedupGroupIDTest{
		// queue name must end with .fifo to be a FIFO queue
		// DeduplicationID and GroupID attributes are supported only by FIFO queues
		queueName: t.Name() + "_queue.fifo",
	}
	test.Setup(t, &sqsconfig.SQSQueueConfig{
		Name:    test.queueName,
		GroupID: "${%table%}s-${id}-${field_int}",
	})

	defer test.TearDown()

	test.runTest(t, test)
}

func (d *DedupGroupIDTest) QueueNames() []string { return []string{d.queueName} }

func (d *DedupGroupIDTest) act() {
	d.tt = &TestTable{
		ID:        1,
		FieldInt:  3,
		FieldData: []byte{4, 5, 6},
	}
	xids := d.mustInsert(d.tt)
	d.xid = xids[0]
}

func (d *DedupGroupIDTest) assert(t *testing.T) {
	messages := d.receiveMessages(t, d, 1*time.Second)
	require.Len(t, messages, 1)
	queueMessages, exists := messages[d.queueName]
	require.True(t, exists)
	require.Len(t, queueMessages, 1)
	msg := queueMessages[0]
	require.Len(t, msg.Attributes, 2)

	dedupID, exists := msg.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)]

	require.True(t, exists)

	decodedID, err := base64.StdEncoding.DecodeString(dedupID)

	require.NoError(t, err)

	dedupParts := strings.Split(string(decodedID), ":")

	require.Len(t, dedupParts, 4)
	assert.Equal(t, "public."+d.testCtx.pgTestOpts.Table, dedupParts[0])
	assert.Equal(t, fmt.Sprintf("%d", d.tt.ID), dedupParts[1])
	assert.Equal(t, fmt.Sprintf("%d", pgwal.Insert), dedupParts[2])
	assert.Equal(t, fmt.Sprintf("%d", d.xid), dedupParts[3])

	groupID, exists := msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]

	require.True(t, exists)
	assert.Equal(t, fmt.Sprintf("%ss-%d-%d", d.testCtx.pgTestOpts.Table, d.tt.ID, d.tt.FieldInt), groupID)
}

func TestFilterInvalidSymbols(t *testing.T) {
	testCases := []struct {
		name     string
		text     string
		expected string
	}{
		// The length of MessageGroupId is 128 characters.
		{name: "length_more_128", text: strings.Repeat("1", 130), expected: strings.Repeat("1", 128)},

		// Valid values: alphanumeric characters and punctuation (!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~)
		{name: "invalid_symbols", text: "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~text" + string(rune(1)), expected: "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~text"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			test := &FilterInvalidSymbolsTest{ //nolint
				queueName: strings.ReplaceAll(t.Name(), "/", "") + "_queue.fifo",
				fieldText: testCase.text,
				expected:  testCase.expected,
			}

			test.Setup(t, &sqsconfig.SQSQueueConfig{ //nolint
				Name:    test.queueName,
				GroupID: "${field_text}",
			})
			defer test.TearDown()
			test.runTest(t, test)
		})
	}
}

type FilterInvalidSymbolsTest struct {
	sqsTest
	queueName string
	tt        *TestTable
	fieldText string
	expected  string
}

func (s *FilterInvalidSymbolsTest) QueueNames() []string { return []string{s.queueName} }

func (s *FilterInvalidSymbolsTest) act() {
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf("ALTER TABLE%s%s ADD field_text text null", " ", s.testCtx.pgTestOpts.Table))
	require.NoError(s.testCtx.t, err)
	s.tt = &TestTable{
		ID:        1,
		FieldInt:  3,
		FieldData: []byte{4, 5, 6},
	}

	query := fmt.Sprintf("INSERT INTO%s%s (id, field_int, field_data, field_text) VALUES ($1, $2, $3, $4)", " ", s.testCtx.pgTestOpts.Table)
	_, err = s.pool.Exec(context.Background(), query, s.tt.ID, s.tt.FieldInt, s.tt.FieldData, s.fieldText)
	require.NoError(s.testCtx.t, err)
}

func (s *FilterInvalidSymbolsTest) assert(t *testing.T) {
	messages := s.receiveMessages(t, s, 5*time.Second)
	require.Len(t, messages, 1)

	queueMessages, exists := messages[s.queueName]
	require.True(t, exists)
	require.Len(t, queueMessages, 1)

	msg := queueMessages[0]
	dedupID, exists := msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	require.True(t, exists)
	assert.Equal(t, s.expected, dedupID)
}

func TestMultipleTables(t *testing.T) {
	test := &MultipleTablesTest{
		queueNames: []string{
			"users_queue",
			"orders_queue",
		},
		tableNames: []string{
			"users",
			"orders",
		},
		user: &multipleTablesUser{
			userID: 1,
			name:   "user1",
		},
		order: &multipleTablesOrder{
			orderID:   1,
			userID:    1,
			orderDate: time.Now(),
		},
	}

	test.MustSetup(t, test)
	defer test.TearDown()

	test.runTest(t, test)
}

type MultipleTablesTest struct {
	sqsTest
	queueNames []string
	tableNames []string
	user       *multipleTablesUser
	order      *multipleTablesOrder
}

func (mt *MultipleTablesTest) act() {
	mt.user = &multipleTablesUser{userID: 1, name: "user1"}
	mt.order = &multipleTablesOrder{orderID: 1, userID: 1, orderDate: time.Now()}

	tx, err := mt.pool.Begin(mt.testCtx.runCtx)
	if err != nil {
		mt.log.Fatal("failed to start tx", zap.Error(err))
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(mt.testCtx.runCtx, "insert into users (userid, username) values ($1, $2)", mt.user.userID, mt.user.name)
	if err != nil {
		mt.log.Fatal("failed to insert users", zap.Error(err))
	}

	_, err = tx.Exec(mt.testCtx.runCtx,
		"insert into orders (orderid, userid, orderdate) values ($1, $2, $3)",
		mt.order.orderID, mt.order.userID, mt.order.orderDate)

	if err != nil {
		mt.log.Fatal("failed to insert orders", zap.Error(err))
	}

	err = tx.Commit(mt.testCtx.runCtx)
	if err != nil {
		mt.log.Fatal("failed to commit", zap.Error(err))
	}
}

func (mt *MultipleTablesTest) assert(t *testing.T) {
	allMsg := mt.receiveMessages(t, mt, 5*time.Second)
	require.Len(t, allMsg, 2)
}

func (mt *MultipleTablesTest) TableNames() []string {
	return mt.tableNames
}

func (mt *MultipleTablesTest) QueueNames() []string {
	return mt.queueNames
}

func (mt *MultipleTablesTest) CreateScripts() []string {
	return []string{
		"create table users (userid int primary key, username text not null)",
		"create table orders (orderid int primary key, userid int not null, orderdate timestamp)",
	}
}

func (mt *MultipleTablesTest) UpdateConfig(appConfig *sqsconfig.SQSAppConfig) {
	appConfig.Tables["public.users"].QueueConfig = &sqsconfig.SQSQueueConfig{
		Name: mt.queueNames[0],
	}
	appConfig.Tables["public.orders"].QueueConfig = &sqsconfig.SQSQueueConfig{
		Name: mt.queueNames[1],
	}
}

type multipleTablesUser struct {
	userID int
	name   string
}

type multipleTablesOrder struct {
	orderID   int
	userID    int
	orderDate time.Time
}

// I don't remember why I created this test in the first place or why I didn't finish it
// so leaving it commented out for now
//func TestSkipFailedTxTest(t *testing.T) {
//	test := &SkipFailedTxTest{
//		queueName: t.Name() + "_queue.fifo",
//	}
//
//	test.Setup(t, &sqsconfig.SQSQueueConfig{
//		Name: test.queueName,
//	})
//	test.runTest(t, test)
//}

func (s *SkipFailedTxTest) QueueNames() []string { return []string{s.queueName} }

func (s *SkipFailedTxTest) act() {

}

func (s *SkipFailedTxTest) assert(_ *testing.T) {

}

type SkipFailedTxTest struct {
	sqsTest
	queueName string
}

func (s *sqsTest) receiveMessages(t *testing.T, queueReader TestQueueReader, timeout time.Duration) map[string][]types.Message {
	res := map[string][]types.Message{}

	for _, queueName := range queueReader.QueueNames() {
		queueURL, _, err := sqsops.ReadOrCreateSQSQueueURL(t.Context(), s.sqsClient, queueName, s.log)
		require.NoError(t, err)

		getRes := func() {
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()

			s.log.Info("ReceiveMessage start", zap.String("queue_url", *queueURL))

			recRes, err := s.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     10,
				QueueUrl:            queueURL,
				MessageSystemAttributeNames: []types.MessageSystemAttributeName{
					types.MessageSystemAttributeNameMessageDeduplicationId,
					types.MessageSystemAttributeNameMessageGroupId,
				},
			})

			require.NoError(t, err)

			s.log.Info("ReceiveMessage end", zap.String("queue_url", *queueURL), zap.Int("messages_count", len(recRes.Messages)))

			res[queueName] = recRes.Messages
		}

		getRes()
	}

	return res
}

func (s *sqsTest) runTest(t *testing.T, runner TestRunner) {
	metrics.UnregisterAllMetrics()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	repl := replicator.MustCreateWithLogger(replicator.NewOptions(s.testCtx.pgTestOpts.Slot, s.testCtx.pgTestOpts.Pub, 500*time.Millisecond, 1*time.Second, 10*time.Second, 10000),
		s.testCtx.listener.Listener(),
		pg2stats.NewNop(),
		s.connector,
		"pg2sqs_test",
		s.log)

	doneChan := make(chan error)

	defer close(doneChan)

	err := repl.Start(ctx, doneChan)

	require.NoError(t, err)

	s.testCtx.listener.Start(ctx)

	s.log.Debug("act start")
	runner.act()
	s.log.Debug("act end")

	endXid := s.mustReadXid()

	s.log.Debug("got endXid", zap.Uint32(pglogger.XIDParam, endXid))

	// -1 because we need a previous transaction
	repl.WatchXID = endXid - 1

	s.testCtx.listener.logger.Info("WatchXID", zap.Uint32(pglogger.XIDParam, repl.WatchXID))
	timeout, cancelT := context.WithTimeout(context.Background(), time.Minute)
	defer cancelT()
	timedOut := false

	select {
	case _ = <-doneChan:
		break
	case _ = <-timeout.Done():
		timedOut = true
	}

	require.False(t, timedOut, "test has timed out")

	t.Log(t.Name(), ":", "graceful shutdown")
	err = repl.GracefulShutdown(context.Background())
	require.NoError(t, err)
	// stop replicator
	cancel()

	runner.assert(t)
}

func (s *sqsTest) mustReadXid() uint32 {
	r, err := s.pool.Query(context.Background(), "SELECT txid_current()")
	if err != nil {
		panic(fmt.Errorf("failed to read current xid: %v", err))
	}

	defer r.Close()

	r.Next()

	var xid uint32
	err = r.Scan(&xid)

	if err != nil {
		panic(fmt.Errorf("failed to read xid: %v", err))
	}

	return xid
}
