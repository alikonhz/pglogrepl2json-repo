package listeners

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgCont "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

const (
	commitTxMsg = "could not get commit timestamp data. Make sure the configuration parameter \"track_commit_timestamp\" is set"
)

type streamData struct {
	insertValue []byte
	updateValue []byte
}

func TestSimpleTx(t *testing.T) {

	// JSON channel, we're expecting 3 entries
	c := make(chan []byte, 3)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertSimpleData)

	assert.Equal(t, 3, len(receivedJSON))
	assertBegin(t, xid, commitTime, receivedJSON[0])
	assertSimpleInsert(t, tOpts.Table, receivedJSON[1])
	assertCommit(t, receivedJSON[2])
}

func TestTxWithUpdate(t *testing.T) {
	c := make(chan []byte, 4)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertUpdateSimpleData)

	assert.Equal(t, 4, len(receivedJSON))
	assertBegin(t, xid, commitTime, receivedJSON[0])
	assertSimpleInsert(t, tOpts.Table, receivedJSON[1])
	assertSimpleUpdate(t, tOpts.Table, receivedJSON[2])
	assertCommit(t, receivedJSON[3])
}

func TestTxWithDelete(t *testing.T) {
	// begin, insert, commit -> first tx
	// begin, delete, commit -> second tx
	c := make(chan []byte, 6)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertDeleteSimpleData)
	assert.Equal(t, 6, len(receivedJSON))

	assertBegin(t, xid, commitTime, receivedJSON[3])
	assertSimpleDelete(t, tOpts.Table, receivedJSON[4])
	assertCommit(t, receivedJSON[5])
}

func TestTxWithTruncate(t *testing.T) {
	c := make(chan []byte, 6)
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, insertTruncateSimpleData)

	assert.Equal(t, 6, len(receivedJSON))

	assertBegin(t, xid, commitTime, receivedJSON[3])
	assertTruncate(t, tOpts.Table, receivedJSON[4])
	assertCommit(t, receivedJSON[5])
}

func TestTxWithLogicalTransactionalMessage(t *testing.T) {

	t.Run("logical decoding msg hex (transaction)", func(t *testing.T) {
		testTxWithLogicalTranMsg(t, decode.BinaryEncodingHex)
	})

	t.Run("logical decoding msg base64 (transaction)", func(t *testing.T) {
		testTxWithLogicalTranMsg(t, decode.BinaryEncodingBase64)
	})
}

func TestTxStream(t *testing.T) {
	// streaming of in-progress transactions is supported since PG version 14
	// streaming is controlled by the "logical_decoding_work_mem" PG config parameter
	// for this test to work it should be set to minimum value (64Kb)
	// call to integrationtest.MustLogicalDecodingWorkMem() makes sure that it's the case
	integrationtest.MustLogicalDecodingWorkMem(t.Context())

	// stream start, insert, stream stop, stream start, update, stream stop, stream commit
	c := make(chan []byte, 7)
	xData := &streamData{}
	tOpts, xid, commitTime, receivedJSON := runTest(t, c, func(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
		return testStream(ctx, tOpts, xData)
	})

	assert.Equal(t, 7, len(receivedJSON))
	assertStreamStart(t, xid, true, receivedJSON[0])
	assertInsert(t, tOpts.Table, xData.insertValue, receivedJSON[1])
	assertStreamStop(t, receivedJSON[2])
	assertStreamStart(t, xid, false, receivedJSON[3])
	assertUpdate(t, tOpts.Table, xData.insertValue, xData.updateValue, receivedJSON[4])
	assertStreamStop(t, receivedJSON[5])
	assertStreamCommit(t, xid, commitTime, receivedJSON[6])
}

func assertStreamCommit(t *testing.T, xid uint32, commitTime time.Time, commitJSON string) {
	m := mustParseToMap(commitJSON)
	assert.Equal(t, StreamCommit, m[ActionKey])
	assertValInMap(t, m, XidKey, xid)
	assertTimestamp(t, m, TimestampKey, TimestampFormat, commitTime)
}

func assertUpdate(t *testing.T, table string, oldData []byte, newData []byte, updateJSON string) {
	oldValMap, newValMap := assertSimpleUpdate(t, table, updateJSON)
	assertSliceOfBytes(t, oldValMap, "field_data", oldData)
	assertSliceOfBytes(t, newValMap, "field_data", newData)
}

func assertInsert(t *testing.T, table string, data []byte, insertJSON string) {
	valMap := assertSimpleInsert(t, table, insertJSON)
	assertSliceOfBytes(t, valMap, "field_data", data)
}

func assertSliceOfBytes(t *testing.T, m map[string]any, key string, data []byte) {
	valStr, ok := m[key].(string)
	assert.True(t, ok, fmt.Sprintf("field_data is not string"))
	valData, err := base64.StdEncoding.DecodeString(valStr)
	assert.NoError(t, err, "field_data is not valid base64 string")
	assert.Equal(t, data, valData)
}

func assertStreamStart(t *testing.T, xid uint32, firstSegment bool, startJSON string) {
	m := mustParseToMap(startJSON)
	assert.Equal(t, StreamStart, m[ActionKey])
	assertValInMap(t, m, XidKey, xid)
	assert.Equal(t, firstSegment, m[FirstSegmentKey])
}

func assertStreamStop(t *testing.T, stopJSON string) {
	m := mustParseToMap(stopJSON)
	assert.Equal(t, StreamStop, m[ActionKey])
}

func testStream(ctx context.Context, tOpts integrationtest.PGTestOptions, xData *streamData) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		const kb100 = 100 * 1024
		data := generateRandomData(kb100)
		xData.insertValue = data

		_, err := tx.Exec(ctx,
			fmt.Sprintf("insert into %s (id, field_int, field_data) values (1, 1, $1)", tOpts.Table),
			data,
		)
		if err != nil {
			return fmt.Errorf("unable to insert data: %w", err)
		}

		data = generateRandomData(kb100)
		xData.updateValue = data

		_, err = tx.Exec(ctx,
			fmt.Sprintf("update %s set field_data = $1, field_int = 2 where id = 1", tOpts.Table),
			data,
		)
		if err != nil {
			return fmt.Errorf("unable to update data: %w", err)
		}

		return nil
	})
}

func generateRandomData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func testTxWithLogicalTranMsg(t *testing.T, msgContentEnc decode.BinaryEncoding) {

	c := make(chan []byte, 4)
	tOpts, xid, commitTime, receivedJSON := runTestWithOpts(t,
		c,
		insertWithTransactionalMessage,
		ListenerJSONOptions{BinaryContentFormat: msgContentEnc},
	)

	assert.Equal(t, 4, len(receivedJSON))
	assertBegin(t, xid, commitTime, receivedJSON[0])
	assertSimpleInsert(t, tOpts.Table, receivedJSON[1])
	assertMsg(t, receivedJSON[2], true, msgContentEnc)
	assertCommit(t, receivedJSON[3])
}

func TestTxWithLogicalMessage(t *testing.T) {
	t.Run("logical decoding msg hex", func(t *testing.T) {
		testTxWithLogicalMsg(t, decode.BinaryEncodingHex)
	})

	t.Run("logical decoding msg base64", func(t *testing.T) {
		testTxWithLogicalMsg(t, decode.BinaryEncodingBase64)
	})
}

func testTxWithLogicalMsg(t *testing.T, msgContentEnc decode.BinaryEncoding) {
	c := make(chan []byte, 4)
	tOpts, xid, commitTime, receivedJSON := runTestWithOpts(t,
		c,
		insertWithMessage,
		ListenerJSONOptions{BinaryContentFormat: msgContentEnc},
	)

	assert.Len(t, receivedJSON, 4)
	assertMsg(t, receivedJSON[0], false, msgContentEnc)
	assertBegin(t, xid, commitTime, receivedJSON[1])
	assertSimpleInsert(t, tOpts.Table, receivedJSON[2])
	assertCommit(t, receivedJSON[3])
}

func runTestWithOpts(t *testing.T,
	c chan []byte,
	test func(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32,
	opts ListenerJSONOptions) (integrationtest.PGTestOptions, uint32, time.Time, []string) {

	ctx, cancel := context.WithCancel(t.Context())
	tOpts := integrationtest.MustCreateTestOptions(ctx, "json_test")
	doneChan := make(chan error)

	defer cancel()
	defer close(doneChan)

	jsonListener := MustCreateNewJSON(c, opts)

	//logger, err := zap.NewDevelopment()
	logger := zap.NewNop()
	connector, err := pgconnector.New(tOpts.Opts, logger)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(t.Context()))
	t.Cleanup(func() {
		connector.Close(t.Context())
	})

	repl := replicator.MustCreateWithLogger(CreateReplOptions(tOpts), jsonListener, jsonListener, connector, "test_app_name", logger)

	err = repl.Start(ctx, doneChan)
	require.NoError(t, err)

	xid := test(ctx, tOpts)
	commitTime, err := readCommitTime(ctx, xid)
	assert.NoError(t, err, commitTxMsg)
	assert.NotEqual(t, 0, xid)

	receivedJSON := waitForTestDataWithin(time.Second, c)

	repl.WatchXID = xid

	<-doneChan

	return tOpts, xid, commitTime, receivedJSON
}

func CreateReplOptions(t integrationtest.PGTestOptions) replicator.Options {
	return replicator.NewOptions(t.Slot, t.Pub, t.Timeout, t.Timeout, t.Timeout, 1000)
}

func runTest(t *testing.T, c chan []byte, test func(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32) (integrationtest.PGTestOptions, uint32, time.Time, []string) {
	return runTestWithOpts(t, c, test, ListenerJSONOptions{})
}

func readCommitTime(ctx context.Context, xid uint32) (time.Time, error) {
	conn := integrationtest.MustCreateDbConnectionFromEnv(ctx)
	defer conn.Close()

	res := conn.QueryRow(ctx, "select pg_xact_commit_timestamp($1)", xid)
	var t time.Time
	err := res.Scan(&t)
	return t, err
}

func assertMsg(t *testing.T, msgJSON string, transactional bool, msgContentEnc decode.BinaryEncoding) {
	m := mustParseToMap(msgJSON)
	assert.Equal(t, "M", m[ActionKey])
	assert.Equal(t, transactional, m[TransactionalKey])
	assert.Equal(t, "test", m[PrefixKey])

	contentAny := m[ContentKey]
	assert.NotNil(t, contentAny, fmt.Sprintf("%q is nill", ContentKey))
	content, ok := contentAny.(string)
	assert.True(t, ok, fmt.Sprintf("%q is not string", ContentKey))

	var decodedContent string
	switch msgContentEnc {
	case decode.BinaryEncodingHex:
		decodedContent = mustDecodeFromHex(content)
	case decode.BinaryEncodingBase64:
		decodedContent = mustDecodeFromBase64(content)
	default:
		panic(fmt.Errorf("unsupported logical message content encoding: %d", msgContentEnc))
	}

	assert.Equal(t, "test_message", decodedContent)
}

func mustDecodeFromHex(content string) string {
	data, err := hex.DecodeString(content)
	if err != nil {
		panic(err)
	}

	return string(data)
}

func mustDecodeFromBase64(content string) string {
	data, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		panic(err)
	}

	return string(data)
}

func assertSimpleInsert(t *testing.T, table string, insertJSON string) map[string]any {
	m := mustParseToMap(insertJSON)
	assert.Equal(t, Insert, m[ActionKey])
	assert.Equal(t, "public", m[SchemaKey])
	assert.Equal(t, table, m[TableKey])
	valMap, err := readMapFromKey(m, ColumnsKey)
	assert.NoError(t, err)
	assertValInMap(t, valMap, "id", 1)
	assertValInMap(t, valMap, "field_int", 1)

	return valMap
}

func assertSimpleUpdate(t *testing.T, table string, updateJSON string) (oldValMap map[string]any, newValMap map[string]any) {
	m := mustParseToMap(updateJSON)
	assert.Equal(t, Update, m[ActionKey])
	assert.Equal(t, "public", m[SchemaKey])
	assert.Equal(t, table, m[TableKey])

	var err error

	newValMap, err = readMapFromKey(m, ColumnsKey)
	assert.NoError(t, err)
	assertValInMap(t, newValMap, "id", 1)
	assertValInMap(t, newValMap, "field_int", 2)

	oldValMap, err = readMapFromKey(m, IdentityKey)
	assert.NoError(t, err)
	assertValInMap(t, oldValMap, "id", 1)
	assertValInMap(t, oldValMap, "field_int", 1)

	return oldValMap, newValMap
}

func assertTruncate(t *testing.T, table string, truncateJSON string) {
	m := mustParseToMap(truncateJSON)
	assert.Equal(t, Truncate, m[ActionKey])
	assert.Equal(t, "public", m[SchemaKey])
	assert.Equal(t, table, m[TableKey])
}

func assertSimpleDelete(t *testing.T, table string, deleteJSON string) {
	m := mustParseToMap(deleteJSON)
	assert.Equal(t, Delete, m[ActionKey])
	assert.Equal(t, "public", m[SchemaKey])
	assert.Equal(t, table, m[TableKey])

	// in delete there's only "identity" (i.e. previous values)
	assert.Nil(t, m[ColumnsKey])
	oldValMap, err := readMapFromKey(m, IdentityKey)
	assert.NoError(t, err)
	assertValInMap(t, oldValMap, "id", 1)
	assertValInMap(t, oldValMap, "field_int", 1)
}

func readMapFromKey(m map[string]any, key string) (map[string]any, error) {
	valMapAny, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("%q is not found", key)
	}
	valMap, ok := valMapAny.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%q is not a map[string]any", key)
	}

	return valMap, nil
}

func assertTimestamp(t *testing.T, m map[string]any, key, format string, expectedTime time.Time) {
	ts, ok := m[key]
	assert.True(t, ok, fmt.Sprintf("key %q is not found", key))
	tsStr, ok := ts.(string)
	assert.True(t, ok, fmt.Sprintf("key %q is not a string", key))
	timeJSON, err := time.Parse(format, tsStr)
	assert.NoError(t, err, "invalid format")
	assert.Equal(t, expectedTime, timeJSON)
}

func assertCommit(t *testing.T, commitJSON string) {
	m := mustParseToMap(commitJSON)
	assert.Equal(t, Commit, m[ActionKey])
}

func assertBegin(t *testing.T, xid uint32, commitTime time.Time, beginJSON string) {
	assert.Equal(t, false, commitTime.IsZero(), commitTxMsg)
	m := mustParseToMap(beginJSON)

	assert.Equal(t, Begin, m[ActionKey])
	assertValInMap(t, m, XidKey, xid)
	assertTimestamp(t, m, TimestampKey, TimestampFormat, commitTime)
}

func assertValInMap[T int | uint32 | int64](t *testing.T, m map[string]any, key string, val T) {
	f, ok := m[key].(float64)
	assert.True(t, ok, fmt.Sprintf("%s is not a float64", key))
	i := T(f)
	assert.Equal(t, val, i)
}

func mustParseToMap(j string) map[string]any {
	m := make(map[string]any)
	err := json.Unmarshal([]byte(j), &m)
	if err != nil {
		panic(err)
	}

	return m
}

func waitForTestDataWithin(t time.Duration, c chan []byte) []string {
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	receivedJSON := make([]string, 0)
f:
	for {
		select {
		case <-ctx.Done():
			return receivedJSON
		case j, ok := <-c:
			if !ok {
				break f
			}
			receivedJSON = append(receivedJSON, string(j))
		}
	}

	return receivedJSON
}

func insertUpdateSimpleData(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		_, err = tx.Exec(ctx, fmt.Sprintf("update %s set field_int = 2 where id = 1", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to update simple data: %w", err)
		}

		return nil
	})
}

func insertTruncateSimpleData(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
	insertSimpleData(ctx, tOpts)

	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("truncate table %s", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to truncate table: %w", err)
		}

		return nil
	})
}

func insertDeleteSimpleData(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
	insertSimpleData(ctx, tOpts)

	// we need xid only of the last transaction
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("delete from %s where id = 1", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to delete simple data: %w", err)
		}

		return nil
	})
}

func insertWithTransactionalMessage(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		_, err = tx.Exec(ctx, "select pg_logical_emit_message(true, 'test', 'test_message')")
		if err != nil {
			return fmt.Errorf("unable to send transactional message: %w", err)
		}

		return nil
	})
}

func insertWithMessage(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		_, err = tx.Exec(ctx, "select pg_logical_emit_message(false, 'test', 'test_message')")
		if err != nil {
			return fmt.Errorf("unable to send transactional message: %w", err)
		}

		return nil
	})
}

func insertSimpleData(ctx context.Context, tOpts integrationtest.PGTestOptions) uint32 {
	return integrationtest.MustRunWithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (id, field_int) values (1, 1)", tOpts.Table))
		if err != nil {
			return fmt.Errorf("unable to insert simple data: %w", err)
		}

		return nil
	})
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	initScripts := []string{
		filepath.Join("../internal/integrationtest/testdata", "initpg.sql"),
		filepath.Join("../internal/integrationtest/testdata", "initpgv13.sql"),
	}

	pgC, opts, err := integrationtest.CreatePGContainerOpts(ctx, "postgres:17", "json_test", pgCont.WithInitScripts(initScripts...))
	if err != nil {
		panic(fmt.Errorf("failed to start postgres container: %v", err))
	}

	pgConnStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s", opts.Host, opts.Port, opts.Database, opts.User, opts.Password)

	os.Setenv(integrationtest.EnvPgTestConn, pgConnStr)

	defer pgC.Terminate(context.Background())

	//integrationtest.MustLoad("../.env-test")
	integrationtest.Cleanup()
	os.Exit(m.Run())
}
