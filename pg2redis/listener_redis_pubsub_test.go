package pg2redis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	defaultPublishCfg = []redisconfig.RedisCommandConfig{{"PUBLISH", "{%schema%}.{%table%}", "{json:*}"}}
)

func TestPubSub(t *testing.T) {
	test := &PubSubInsertTest{}
	test.Setup(t, testOptsNoTxTime, defaultPublishCfg)
	test.listenPubSub()
	defer test.TearDown()
	test.runTest(t, test)
}

func TestCommitTimeIsSaveInPubSub(t *testing.T) {
	test := &PubSubCommitTimeTest{}
	test.Setup(t, defaultTestOpts, defaultPublishCfg)
	test.listenPubSub()
	defer test.TearDown()
	test.runTest(t, test)
}

type pubSubTest struct {
	pg2RedisTest
	pubSub    *redis.PubSub
	cancel    context.CancelFunc
	msgs      []*redis.Message
	insertTt  *TestTable
	insertXid uint32
	updateTt  *TestTable
	updateXid uint32
}

type PubSubInsertTest struct {
	pubSubTest
}

func (rt *pubSubTest) TearDownAll() {
	rt.TearDown()
}

func (rt *pubSubTest) TearDown() {
	if rt.cancel != nil {
		rt.cancel()
	}
	if rt.pubSub != nil {
		rt.pubSub.Close()
	}
	rt.pg2RedisTest.TearDown()
}

func (rt *pubSubTest) listenPubSub() {
	rt.pubSub = rt.redisClient.Subscribe(context.Background(), fmt.Sprintf("public.%s", rt.testCtx.pgTestOpts.Table))
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	ch := rt.pubSub.Channel()

	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				rt.msgs = append(rt.msgs, msg)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (rt *PubSubInsertTest) act() {
	rt.insertTt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.insertXid = rt.mustInsert(rt.insertTt)

	rt.updateTt = &TestTable{
		ID:        1,
		FieldInt:  5,
		FieldData: []byte{4, 5, 6},
	}
	rt.updateXid = rt.mustUpdate(rt.updateTt)
}

func (rt *PubSubInsertTest) assert(t *testing.T) {
	assert.Equal(t, 2, len(rt.msgs))

	insertMsg := rt.msgs[0]
	updateMsg := rt.msgs[1]

	assert.Equal(t, rt.insertTt.AsJSON(), insertMsg.Payload)
	assert.Equal(t, rt.updateTt.AsJSON(), updateMsg.Payload)
}

func assertCommitTimeInMap(t *testing.T, rt *pg2RedisTest, payload string, xid uint32) {
	var mm map[string]any
	err := json.Unmarshal([]byte(payload), &mm)
	require.NoError(t, err)
	expectedCommitTime := rt.mustReadCommitTime(xid)
	gotCommitTime, ok := mm[rt.listenerConfig.WriterOpts.TxTimeOpts.Name]
	assert.True(t, ok, "no commit time field in payload")
	assert.Equal(t, expectedCommitTime.Format(time.RFC3339Nano), gotCommitTime)
}

type PubSubCommitTimeTest struct {
	pubSubTest
}

func (rt *PubSubCommitTimeTest) act() {
	rt.insertTt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.insertXid = rt.mustInsert(rt.insertTt)

	rt.updateTt = &TestTable{
		ID:        1,
		FieldInt:  5,
		FieldData: []byte{4, 5, 6},
	}
	rt.updateXid = rt.mustUpdate(rt.updateTt)
}

func (rt *PubSubCommitTimeTest) assert(t *testing.T) {
	insertMsg := rt.msgs[0]
	updateMsg := rt.msgs[1]

	assertCommitTimeInMap(t, &rt.pg2RedisTest, insertMsg.Payload, rt.insertXid)
	assertCommitTimeInMap(t, &rt.pg2RedisTest, updateMsg.Payload, rt.updateXid)
}
