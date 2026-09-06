package pg2redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlexibleMapping(t *testing.T) {
	test := &FlexibleMappingTest{
		tableNames: []string{"users", "orders"},
		createScripts: []string{
			"create table users (userid text primary key, username text not null)",
			"create table orders (orderid text primary key, userid text not null, orderdate timestamp)",
		},
	}

	test.MustSetup(t, test, testOptsNoTxTime, []redisconfig.RedisCommandConfig{
		{"HSET", "public.users:{%pk%}", "{pairs:*}"},
		{"HSET", "public.orders:{%pk%}", "{pairs:*}"},
	})

	defer test.TearDown()
	test.runTest(t, test)
}

type user struct {
	userID   string
	userName string
}

type order struct {
	orderID   string
	userID    string
	orderDate time.Time
}

type FlexibleMappingTest struct {
	pg2RedisTest

	tableNames    []string
	createScripts []string

	user  *user
	order *order
}

func (test *FlexibleMappingTest) act() {
	test.user = &user{userID: "1", userName: "user1"}
	test.order = &order{orderID: "1", userID: "1", orderDate: time.Now()}

	tx, err := test.pool.Begin(test.testCtx.runCtx)
	if err != nil {
		panic(err)
	}

	defer tx.Rollback(context.Background())

	_, err = tx.Exec(test.testCtx.runCtx, "insert into users (userid, username) values ($1, $2)", test.user.userID, test.user.userName)
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec(test.testCtx.runCtx,
		"insert into orders (orderid, userid, orderdate) values ($1, $2, $3)",
		test.order.orderID, test.order.userID, test.order.orderDate)

	if err != nil {
		panic(err)
	}

	err = tx.Commit(test.testCtx.runCtx)
	if err != nil {
		panic(err)
	}
}

func (test *FlexibleMappingTest) assert(t *testing.T) {
	usersEntryKey := fmt.Sprintf("public.users:%s", test.user.userID)
	userMap, err := test.redisClient.HGetAll(t.Context(), usersEntryKey).Result()

	require.NoError(t, err)
	assert.Equal(t, test.user.userID, userMap["userid"])
	assert.Equal(t, test.user.userName, userMap["username"])

	ordersEntryKey := fmt.Sprintf("public.orders:%s", test.order.orderID)
	orderJSON, err := test.redisClient.Get(t.Context(), ordersEntryKey).Result()

	require.NoError(t, err)
	assert.Equal(t, test.order.AsJSON(), orderJSON)
}

func (test *FlexibleMappingTest) UpdateConfig(appConfig *redisconfig.RedisAppConfig) {
	if _, ok := appConfig.Tables["public.users"]; !ok {
		appConfig.Tables["public.users"] = &redisconfig.RedisTableConfig{}
	}
	if _, ok := appConfig.Tables["public.orders"]; !ok {
		appConfig.Tables["public.orders"] = &redisconfig.RedisTableConfig{}
	}
	appConfig.Tables["public.users"].Commands = convertCommands([]redisconfig.RedisCommandConfig{{"HSET", "public.users:{%pk%}", "{pairs:*}"}})
	appConfig.Tables["public.orders"].Commands = convertCommands([]redisconfig.RedisCommandConfig{{"SET", "public.orders:{%pk%}", "{json:*}"}})
}

func (test *FlexibleMappingTest) TableNames() []string {
	return test.tableNames
}

func (test *FlexibleMappingTest) CreateScripts() []string {
	return test.createScripts
}

func (o *order) AsJSON() string {
	m := o.AsMap()
	j, err := m.MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("failed to marshal as json: %v", err))
	}

	return string(j)
}

func (o *order) AsMap() *orderedmap.OrderedMap {
	m := newTestOrderedMap("orderid", "userid", "orderdate")
	m.Set("orderid", o.orderID)
	m.Set("userid", o.userID)
	m.Set("orderdate", o.orderDate.Format("2006-01-02T15:04:05.999999Z"))

	return m
}
