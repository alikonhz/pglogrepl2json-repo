package pg2redis

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZIncrBy(t *testing.T) {
	test := ZIncrByCounterTest{}
	test.Setup(t, testOptsNoTxTime, []redisconfig.RedisCommandConfig{}, test.updateConfig)
	defer test.TearDown()
	test.runTest(t, &test)
}

func DONOTTestInsertZINCRBy(t *testing.T) {
	test := ZIncrByTest{}
	test.Setup(t, testOptsNoTxTime, []redisconfig.RedisCommandConfig{{"ZINCRBY", "{%schema%}.{%table%}", "{diff:field_int}", "{id}"}}, test.updateZIncrByConfigDefault)
	defer test.TearDown()
	test.runTest(t, &test)
}

func (rt *pg2RedisTest) updateZIncrByConfigDefault(config *redisconfig.RedisAppConfig) {
	config.Tables = make(map[string]*redisconfig.RedisTableConfig)
	config.Tables[rt.testCtx.pgTestOpts.TableKey()] = &redisconfig.RedisTableConfig{
		Options: map[string]string{
			"zincrby-key":  "id",
			"zincrby-id":   "id",
			"zincrby-incr": "field_int",
		},
		Commands: convertCommands([]redisconfig.RedisCommandConfig{{"ZINCRBY", "{%schema%}.{%table%}", "{diff:field_int}", "{id}"}}),
	}
}

func DONOTTestInsertThenUpdate(t *testing.T) {
	tests := []struct {
		insertVal int
		updateVal int
		sameTx    bool
		testName  string
	}{
		{insertVal: 5, updateVal: 9, sameTx: true, testName: "InsertThenIncreaseSameTX"},
		{insertVal: 10, updateVal: 2, sameTx: true, testName: "InsertThenDecreaseSameTX"},
		{insertVal: 5, updateVal: 9, sameTx: false, testName: "InsertThenIncrease"},
		{insertVal: 10, updateVal: 2, sameTx: false, testName: "InsertThenDecrease"},
	}

	for _, testRun := range tests {
		t.Run(testRun.testName, func(t *testing.T) {
			test := ZIncrByInsertUpdate{
				insertVal: testRun.insertVal,
				updateVal: testRun.updateVal,
				sameTx:    testRun.sameTx,
			}
			test.Setup(t, testOptsNoTxTime, []redisconfig.RedisCommandConfig{{"ZINCRBY", "public.users", "{diff:field_int}", "{id}"}}, test.updateZIncrByConfigDefault)
			defer test.TearDown()
			test.runTest(t, &test)
		})
	}
}

func DONOTTestUserScoreZINCRBy(t *testing.T) {
	dataTypes := []struct {
		dataType string
		values1  []float64
		values2  []float64
		testName string
	}{
		//{dataType: "smallint", values1: []float64{10, 35, 50, 5}, values2: []float64{11, 33, -8, 12}},
		//{dataType: "integer", values1: []float64{10, 35, 50, 5}, values2: []float64{11, 33, -8, 12}},
		//{dataType: "bigint", values1: []float64{10, 35, 50, 5}, values2: []float64{11, 33, -8, 12}},
		//
		//{dataType: "decimal", values1: []float64{10.11, 3.22, 50.33, 5.444}, values2: []float64{11.5, 33.66, -8.777, 12.8888}},
		//{dataType: "numeric", values1: []float64{10.11, 3.22, 50.33, 5.444}, values2: []float64{11.5, 33.66, -8.777, 12.8888}},
		//{dataType: "numeric", values1: []float64{10.11, 3.22, 50.33, 1}, values2: []float64{11, math.NaN(), math.Inf(-1), math.Inf(1), math.Inf(1)}, testName: "numeric_overflow"},
		{dataType: "real", values1: []float64{10, 3, 50, 5}, values2: []float64{11, 33, -8, 12}},
		//{dataType: "double precision", values1: []float64{10.11, 3.22, 50.33, 5.444}, values2: []float64{11.5, 33.66, -8.777, 12.8888}},
	}

	id1 := 1
	id2 := 2

	for _, testRun := range dataTypes {
		var testName string
		if testRun.testName == "" {
			testName = fmt.Sprintf("datatype_%s", testRun.dataType)
		} else {
			testName = testRun.testName
		}
		idX1 := id1
		idX2 := id2
		t.Run(testName, func(t *testing.T) {

			test := ZIncrByUserScore{
				dataType:    testRun.dataType,
				user1Scores: testRun.values1,
				user2Scores: testRun.values2,
				id1:         idX1,
				id2:         idX2,
			}
			test.SetupUserScore(t)
			defer test.TearDownUserScore()
			test.runTest(t, &test)
		})

		id1 += 2
		id2 += 2
	}
}

type ZIncrByTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *ZIncrByTest) act() {
	rt.tt = &TestTable{
		ID:       6713,
		FieldInt: 8,
	}

	rt.mustInsert(rt.tt)
}

func (rt *ZIncrByTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s", rt.testCtx.pgTestOpts.Table)
	res, err := rt.redisClient.ZRangeWithScores(context.Background(), entryKey, 0, 1).Result()
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, fmt.Sprintf("%d", rt.tt.ID), res[0].Member)
	assert.Equal(t, float64(rt.tt.FieldInt), res[0].Score)
}

type ZIncrByInsertUpdate struct {
	pg2RedisTest
	tt        *TestTable
	insertVal int
	updateVal int
	sameTx    bool
}

func (rt *ZIncrByInsertUpdate) act() {
	if rt.sameTx {
		rt.actSameTx()
	} else {
		rt.actTwoTx()
	}
}

func (rt *ZIncrByInsertUpdate) actSameTx() {
	tx, err := rt.pool.Begin(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(context.Background())

	// first we do insert
	// then we do an update in the same transaction
	// as a result ZINCRBY should equal to the value in update
	tt := &TestTable{
		ID:       53413,
		FieldInt: rt.insertVal,
	}
	rt.mustInsertTx(tt, tx)
	tt = &TestTable{
		ID:       tt.ID,
		FieldInt: rt.updateVal,
	}

	rt.mustUpdateTx(tt, tx)

	rt.tt = tt

	tx.Commit(context.Background())
}

func (rt *ZIncrByInsertUpdate) actTwoTx() {
	// first we do insert
	// then we do an update in a new transaction
	// as a result ZINCRBY should equal to the value in update
	tt := &TestTable{
		ID:       98123,
		FieldInt: rt.insertVal,
	}
	rt.mustInsert(tt)
	tt = &TestTable{
		ID:       tt.ID,
		FieldInt: rt.updateVal,
	}

	rt.mustUpdate(tt)

	rt.tt = tt
}

func (rt *ZIncrByInsertUpdate) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s", rt.testCtx.pgTestOpts.Table)
	res, err := rt.redisClient.ZRangeWithScores(context.Background(), entryKey, 0, 1).Result()
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, fmt.Sprintf("%d", rt.tt.ID), res[0].Member)
	assert.Equal(t, float64(rt.tt.FieldInt), res[0].Score)
}

type ZIncrByUserScore struct {
	pg2RedisTest

	dataType    string
	user1Score  float64
	user1Scores []float64
	user2Score  float64
	user2Scores []float64

	id1 int
	id2 int
}

func (rt *ZIncrByUserScore) SetupUserScore(t *testing.T) {
	rt.Setup(t, testOptsNoTxTime, []redisconfig.RedisCommandConfig{{"ZINCRBY", "public.users_score", "{diff:score}", "{user_id}"}}, rt.updateConfig, rt.createTables)
}

func (rt *ZIncrByUserScore) TearDownUserScore() {
	_, err := rt.pool.Exec(context.Background(), "DROP TABLE IF EXISTS users_score; DROP TABLE IF EXISTS users;")

	if err != nil {
		panic(err)
	}

	_, err = rt.redisClient.Del(context.Background(), "public.users_score").Result()
	if err != nil {
		panic(err)
	}
	rt.TearDown()
}

func (rt *ZIncrByUserScore) act() {
	tx, err := rt.pool.Begin(context.Background())
	if err != nil {
		panic(err)
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(context.Background(), "INSERT INTO users(id, name) VALUES ($1, 'name1')", rt.id1)
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec(context.Background(), "INSERT INTO users(id, name) VALUES ($1, 'name2')", rt.id2)

	insertScores := func(id int, scores []float64) float64 {
		score := 0.0
		for _, nextScore := range scores {
			var val any

			if math.IsInf(nextScore, 1) {
				if rt.pgVersion >= pgschema.V14 {
					val = "infinity"
				} else {
					// infinity for numerics is supported only in PG >= 14
					val = "NaN"
				}
			} else if math.IsInf(nextScore, -1) {
				if rt.pgVersion >= pgschema.V14 {
					val = "-infinity"
				} else {
					// infinity for numerics is supported only in PG >= 14
					val = "NaN"
				}
			} else {
				val = nextScore
			}
			_, err := tx.Exec(context.Background(), "INSERT INTO users_score(user_id, score) VALUES ($1, $2)", id, val)
			if err != nil {
				panic(err)
			}

			if math.IsNaN(nextScore) || math.IsInf(nextScore, 0) {
				continue
			}

			score += nextScore
		}

		return score
	}

	user1Score := insertScores(rt.id1, rt.user1Scores)
	user2Score := insertScores(rt.id2, rt.user2Scores)

	rt.user1Score = user1Score
	rt.user2Score = user2Score
	tx.Commit(context.Background())
}

func (rt *ZIncrByUserScore) assert(t *testing.T) {
	r, err := rt.redisClient.ZRevRangeWithScores(context.Background(), "public.users_score", 0, 1).Result()
	require.NoError(t, err)
	assert.Equal(t, 2, len(r))

	assert.Equal(t, float64(rt.user1Score), r[0].Score)
	assert.Equal(t, fmt.Sprintf("%d", rt.id1), r[0].Member)

	assert.Equal(t, float64(rt.user2Score), r[1].Score)
	assert.Equal(t, fmt.Sprintf("%d", rt.id2), r[1].Member)
}

func (rt *ZIncrByUserScore) updateConfig(config *redisconfig.RedisAppConfig) {
	config.Tables = make(map[string]*redisconfig.RedisTableConfig)
	config.Tables["public.users_score"] = &redisconfig.RedisTableConfig{
		// in ZINCRBY mode first column(s) must be PK, next goes the key, last is the score
		Options: map[string]string{
			"zincrby-key":  "user_id",
			"zincrby-incr": "score",
		},
		Commands: convertCommands([]redisconfig.RedisCommandConfig{{"ZINCRBY", "public.users_score", "{diff:score}", "{user_id}"}}),
	}
}

func (rt *ZIncrByUserScore) createTables(config *redisconfig.RedisAppConfig) {
	_, err := rt.pool.Exec(context.Background(),
		fmt.Sprintf(
			`
CREATE TABLE IF NOT EXISTS users(id bigserial not null primary key, name text not null);
CREATE TABLE IF NOT EXISTS users_score(
	id bigserial not null primary key,
	user_id bigint not null,
	score %s not null,
	created_at timestamp default clock_timestamp(),
	constraint fk_users_score_ref_users foreign key (user_id) references users(id)
);
`, rt.dataType))
	if err != nil {
		panic(err)
	}

	_, err = rt.pool.Exec(context.Background(), fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE users_score", rt.testCtx.pgTestOpts.Pub))
	if err != nil {
		panic(err)
	}
}

type ZIncrByCounterTest struct {
	pg2RedisTest
	tt *TestTable
}

func (rt *ZIncrByCounterTest) updateConfig(config *redisconfig.RedisAppConfig) {
	config.Tables = make(map[string]*redisconfig.RedisTableConfig)
	config.Tables[rt.testCtx.pgTestOpts.TableKey()] = &redisconfig.RedisTableConfig{
		Insert: &redisconfig.TableOperationConfig{
			Commands: convertCommands([]redisconfig.RedisCommandConfig{{"ZINCRBY", "{%schema%}.{%table%}", "1", "{id}"}}),
		},
		Delete: &redisconfig.TableOperationConfig{
			Commands: convertCommands([]redisconfig.RedisCommandConfig{{"ZINCRBY", "{%schema%}.{%table%}", "-1", "{id}"}}),
		},
	}
}

func (rt *ZIncrByCounterTest) act() {
	rt.tt = &TestTable{
		ID:       1001,
		FieldInt: 10, // Value shouldn't matter as we increment by "1"
	}

	// 1. Insert ID 1001
	rt.mustInsert(rt.tt)

	// 2. Insert ID 1002
	tt2 := &TestTable{
		ID:       1002,
		FieldInt: 20,
	}
	rt.mustInsert(tt2)

	// 3. Delete ID 1001
	rt.mustDelete(rt.tt)
}

func (rt *ZIncrByCounterTest) assert(t *testing.T) {
	entryKey := fmt.Sprintf("public.%s", rt.testCtx.pgTestOpts.Table)

	// After Insert(1001) + Insert(1002) + Delete(1001):
	// ID 1001 should have score 0 (1 - 1)
	// ID 1002 should have score 1

	res1001, err := rt.redisClient.ZScore(context.Background(), entryKey, "1001").Result()
	require.NoError(t, err)
	assert.Equal(t, float64(0), res1001)

	res1002, err := rt.redisClient.ZScore(context.Background(), entryKey, "1002").Result()
	require.NoError(t, err)
	assert.Equal(t, float64(1), res1002)
}
