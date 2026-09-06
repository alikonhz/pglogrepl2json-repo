package pg2redis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	defaultSETCmd = []string{"SET", "{%schema%}.{%table%}:{%pk%}", "{json:*}"}
	defaultSETCfg = []redisconfig.RedisCommandConfig{defaultSETCmd}
)

func TestInsertCopiedToSet(t *testing.T) {
	test := &SetInsertTest{}
	test.Setup(t, testOptsNoTxTime, defaultSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestUpdateCopiedToSet(t *testing.T) {
	test := &SetUpdateTest{}
	test.Setup(t, testOptsNoTxTime, defaultSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestDeleteCopiedToSet(t *testing.T) {
	test := &SetDeleteTest{}
	test.Setup(t, testOptsNoTxTime, []redisconfig.RedisCommandConfig{}, test.updateConfig)
	defer test.TearDown()
	test.runTest(t, test)
}

func TestCommitTimeIsSavedToSet(t *testing.T) {
	test := &SetCommitTimeTest{}
	test.Setup(t, defaultTestOpts, defaultSETCfg)
	defer test.TearDown()
	test.runTest(t, test)
}

type SetInsertTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *SetInsertTest) act() {
	rt.tt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.mustInsert(rt.tt)
}

func (rt *SetInsertTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.Get(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, rt.tt.AsJSON(), res)
}

type SetUpdateTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *SetUpdateTest) act() {
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

func (rt *SetUpdateTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.Get(context.Background(), entryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, rt.tt.AsJSON(), res)
}

type SetDeleteTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *SetDeleteTest) updateConfig(appConf *redisconfig.RedisAppConfig) {
	appConf.Tables = map[string]*redisconfig.RedisTableConfig{}
	appConf.Tables[rt.testCtx.pgTestOpts.Table] = &redisconfig.RedisTableConfig{
		Commands: convertCommands(defaultSETCfg),
		Delete: &redisconfig.TableOperationConfig{
			Commands: convertCommands([]redisconfig.RedisCommandConfig{{"DEL", "{%schema%}.{%table%}:{%pk%}"}}),
		},
	}
}

func (rt *SetDeleteTest) act() {
	rt.tt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.mustInsert(rt.tt)
	rt.mustDelete(rt.tt)
}

func (rt *SetDeleteTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.Get(context.Background(), entryKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		require.NoError(t, err)
	}

	assert.Empty(t, res)
}

type SetCommitTimeTest struct {
	pg2RedisTest
	tt  *TestTable
	xid uint32
}

func (rt *SetCommitTimeTest) act() {
	rt.tt = &TestTable{
		ID:        98,
		FieldInt:  76,
		FieldData: []byte{1, 2, 3},
	}
	rt.xid = rt.mustInsert(rt.tt)
}

func (rt *SetCommitTimeTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s:%d", rt.testCtx.pgTestOpts.Table, rt.tt.ID)
	res, err := rt.redisClient.Get(context.Background(), entryKey).Result()
	require.NoError(t, err)

	assertCommitTimeInMap(t, &rt.pg2RedisTest, res, rt.xid)
}
