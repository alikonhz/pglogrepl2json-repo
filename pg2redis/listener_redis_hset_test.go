package pg2redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	defaultHSETCmd = []string{"HSET", "{%schema%}.{%table%}:{%pk%}", "{pairs:*}"}
	defaultHSETCfg = []redisconfig.RedisCommandConfig{defaultHSETCmd}
)

func TestConfiguredColumns(t *testing.T) {
	test := &HSetConfiguredColumnsTest{}
	test.Setup(t, defaultTestOpts, []redisconfig.RedisCommandConfig{}, test.updateConfig)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestInsertCopiedToHSet(t *testing.T) {
	test := &HSetInsertTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestUpdateCopiedToHSet(t *testing.T) {
	test := &HSetUpdateTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestDeleteCopiedToHSet(t *testing.T) {
	test := &HSetDeleteTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg, test.updateConfig)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestStreamedInsertToHSet(t *testing.T) {
	test := &HSetStreamedInsertTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestStreamedUpdateToHSet(t *testing.T) {
	test := &HSetStreamedUpdateTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestStreamedDeleteToHSet(t *testing.T) {
	test := &HSetStreamedDeleteTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg, test.updateConfig)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestCommitTimeIsSavedToHSet(t *testing.T) {
	test := &HSetCommitTimeTest{}
	test.Setup(t, defaultTestOpts, defaultHSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

type HSetInsertTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *HSetInsertTest) act() {
	rt.tt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.mustInsert(rt.tt)
}

func (rt *HSetInsertTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%d", rt.tt.FieldInt), res["field_int"])
	assert.Equal(t, decode.EncodeBinary(rt.tt.FieldData, decode.BinaryEncodingBase64), res["field_data"])

	rt.assertLSNInRedis(t)
}

type HSetUpdateTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *HSetUpdateTest) act() {
	rt.tt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.mustInsert(rt.tt)
	// update
	rt.tt.FieldInt = 5
	rt.mustUpdate(rt.tt)
}

func (rt *HSetUpdateTest) assert(t *testing.T) {
	assertHSetEntry(t, rt.redisClient, rt.tt, rt.testCtx.pgTestOpts.Table)
}

type HSetDeleteTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *HSetDeleteTest) act() {
	rt.tt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.mustInsert(rt.tt)
	rt.mustDelete(rt.tt)
}

func (rt *HSetDeleteTest) assert(t *testing.T) {
	assertNoHSetEntryInRedis(t, rt.redisClient, rt.tt, rt.testCtx.pgTestOpts.Table)
}

func (rt *HSetDeleteTest) updateConfig(appConf *redisconfig.RedisAppConfig) {
	tableKey := pgschema.MakeTableName("public", rt.testCtx.pgTestOpts.Table)
	appConf.Tables = map[string]*redisconfig.RedisTableConfig{}
	appConf.Tables[tableKey.FullName] = &redisconfig.RedisTableConfig{
		Commands: convertCommands(defaultHSETCfg),
		Delete: &redisconfig.TableOperationConfig{
			Commands: convertCommands([]redisconfig.RedisCommandConfig{{"HDEL", tableKey.FullName + ":{%pk%}", "{columns:*}"}}),
		},
	}
}

type HSetStreamedInsertTest struct {
	pg2RedisTest
	tt11 *TestTable
	tt12 *TestTable
	tt21 *TestTable
	tt22 *TestTable
}

func (rt *HSetStreamedInsertTest) act() {
	rt.tt11 = &TestTable{ID: 1, FieldInt: 11, FieldData: generateRandomData(kb100)}
	rt.tt21 = &TestTable{ID: 2, FieldInt: 21, FieldData: generateRandomData(kb100)}
	rt.tt22 = &TestTable{ID: 3, FieldInt: 22, FieldData: generateRandomData(kb100)}
	rt.tt12 = &TestTable{ID: 4, FieldInt: 12, FieldData: generateRandomData(kb100)}

	ctx := context.Background()
	tx1, err := rt.pool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	tx2, err := rt.pool.Begin(ctx)
	if err != nil {
		panic(err)
	}

	defer tx1.Rollback(ctx)
	defer tx2.Rollback(ctx)

	// first TX inserts data
	rt.mustInsertTx(rt.tt11, tx1)

	// then second tx inserts two entries and commits
	// according to test setup this should cause PG streaming protocol to kick in
	rt.mustInsertTx(rt.tt21, tx2)
	rt.mustInsertTx(rt.tt22, tx2)
	err = tx2.Commit(ctx)
	if err != nil {
		panic(err)
	}

	rt.mustInsertTx(rt.tt12, tx1)
	err = tx1.Commit(ctx)
	if err != nil {
		panic(err)
	}

	// make sure that both transactions will be processed
	time.Sleep(300 * time.Millisecond)
}

func (rt *HSetStreamedInsertTest) assert(t *testing.T) {
	assertHSetEntry(t, rt.redisClient, rt.tt11, rt.testCtx.pgTestOpts.Table)
	assertHSetEntry(t, rt.redisClient, rt.tt12, rt.testCtx.pgTestOpts.Table)
	assertHSetEntry(t, rt.redisClient, rt.tt21, rt.testCtx.pgTestOpts.Table)
	assertHSetEntry(t, rt.redisClient, rt.tt22, rt.testCtx.pgTestOpts.Table)
}

type HSetStreamedUpdateTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *HSetStreamedUpdateTest) act() {
	rt.tt = &TestTable{
		ID:        100,
		FieldInt:  1,
		FieldData: []byte{1, 2, 3},
	}

	// simple insert
	rt.mustInsert(rt.tt)

	rt.tt.FieldInt = 100
	rt.tt.FieldData = generateRandomData(kb100)

	tx, err := rt.pool.Begin(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Commit(context.Background())
	rt.mustUpdateTx(rt.tt, tx)
}

func (rt *HSetStreamedUpdateTest) assert(t *testing.T) {
	assertHSetEntry(t, rt.redisClient, rt.tt, rt.testCtx.pgTestOpts.Table)
}

type HSetStreamedDeleteTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *HSetStreamedDeleteTest) act() {
	rt.tt = &TestTable{
		ID:        100,
		FieldInt:  1,
		FieldData: generateRandomData(kb100),
	}

	rt.mustInsert(rt.tt)
	rt.mustDelete(rt.tt)
}

func (rt *HSetStreamedDeleteTest) assert(t *testing.T) {
	assertNoHSetEntryInRedis(t, rt.redisClient, rt.tt, rt.testCtx.pgTestOpts.Table)
}

func (rt *HSetStreamedDeleteTest) updateConfig(appConf *redisconfig.RedisAppConfig) {
	tableKey := pgschema.MakeTableName("public", rt.testCtx.pgTestOpts.Table)
	appConf.Tables = map[string]*redisconfig.RedisTableConfig{}
	appConf.Tables[tableKey.FullName] = &redisconfig.RedisTableConfig{
		Commands: convertCommands(defaultHSETCfg),
		Delete: &redisconfig.TableOperationConfig{
			Commands: convertCommands([]redisconfig.RedisCommandConfig{{"HDEL", tableKey.FullName + ":{%pk%}", "{columns:*}"}}),
		},
	}
}

func assertNoHSetEntryInRedis(t *testing.T, redisClient *redis.Client, tt *TestTable, table string) {
	entryKey := fmt.Sprintf("public.%s:%d", table, tt.ID)
	res, err := redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.Empty(t, res, "expected entry %s to be deleted from Redis", entryKey)
}

func assertHSetEntry(t *testing.T, redisClient *redis.Client, tt *TestTable, table string) {
	entryKey := fmt.Sprintf("public.%s:%d", table, tt.ID)
	res, err := redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, res, "expected entry %s to exist in Redis", entryKey)
	assert.Equal(t, fmt.Sprintf("%d", tt.FieldInt), res["field_int"])
	expected := decode.EncodeBinary(tt.FieldData, decode.BinaryEncodingBase64)
	actual := res["field_data"]
	if expected != actual {
		assert.Failf(t, "field_data did not match expected value", "expected: %s, actual: %s", expected[0:20], actual[0:20])
	}
}

type HSetCommitTimeTest struct {
	pg2RedisTest
	tt  *TestTable
	xid uint32
}

func (rt *HSetCommitTimeTest) act() {
	rt.tt = &TestTable{
		ID:        98,
		FieldInt:  76,
		FieldData: []byte{1, 2, 3},
	}
	rt.xid = rt.mustInsert(rt.tt)
}

func (rt *HSetCommitTimeTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	commitTime, ok := res[rt.listenerConfig.WriterOpts.TxTimeOpts.Name]
	assert.True(t, ok)
	txCommitTime := rt.mustReadCommitTime(rt.xid)
	assert.Equal(t, txCommitTime.Format(time.RFC3339Nano), commitTime)
}

type HSetConfiguredColumnsTest struct {
	pg2RedisTest
	insertTT *TestTable
	updateTT *TestTable
	xid      uint32
}

func (rt *HSetConfiguredColumnsTest) act() {
	rt.insertTT = &TestTable{
		ID:        169,
		FieldInt:  1982,
		FieldData: []byte{1, 2, 3},
	}
	rt.mustInsert(rt.insertTT)

	rt.updateTT = &TestTable{
		ID:       rt.insertTT.ID,
		FieldInt: 2012,
	}
	rt.xid = rt.mustUpdate(rt.updateTT)
}

func (rt *HSetConfiguredColumnsTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.insertTT.ID)
	res, err := rt.redisClient.HGetAll(context.Background(), entryKey).Result()
	require.NoError(t, err)
	// field_int must exist
	val, exists := res["field_int"]
	assert.True(t, exists)
	assert.Equal(t, fmt.Sprintf("%d", rt.updateTT.FieldInt), val)
	// field_data must not exist
	val, exists = res["field_data"]
	assert.False(t, exists)
}

func (rt *HSetConfiguredColumnsTest) updateConfig(appConf *redisconfig.RedisAppConfig) {
	tableKey := pgschema.MakeTableName("public", rt.testCtx.pgTestOpts.Table)
	appConf.Tables = map[string]*redisconfig.RedisTableConfig{}
	appConf.Tables[tableKey.FullName] = &redisconfig.RedisTableConfig{
		Commands: convertCommands([]redisconfig.RedisCommandConfig{{"HSET", tableKey.FullName + ":{%pk%}", "{pairs:id,field_int}"}}),
	}
}
