package pg2redis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedisStream(t *testing.T) {
	test := &StreamTest{}
	test.Setup(t, testOptsNoTxTime, []redisconfig.RedisCommandConfig{{"XADD", "{%schema%}.{%table%}", "*", "{pairs:*}"}})
	defer test.TearDownAll()
	test.listenStream()
	test.runTest(t, test)
}

type StreamTest struct {
	pg2RedisTest
	insertTt *TestTable
	updateTt *TestTable
	cancel   context.CancelFunc
	msgs     []map[string]any
}

func (rt *StreamTest) TearDownAll() {
	if rt.cancel != nil {
		rt.cancel()
	}
	rt.redisClient.Del(context.Background(), fmt.Sprintf("public.%s", rt.testCtx.pgTestOpts.Table))
	rt.TearDown()
}
func (rt *StreamTest) listenStream() {
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	streamName := fmt.Sprintf("public.%s", rt.testCtx.pgTestOpts.Table)

	go func() {
		var lastID = "0"
		end := false
		for !end {
			streams, err := rt.redisClient.XReadStreams(ctx, streamName, lastID).Result()
			if errors.Is(err, context.Canceled) {
				end = true
			} else if err != nil && !errors.Is(err, redis.Nil) {
				panic(err)
			}

			if len(streams) > 0 && len(streams[0].Messages) > 0 {
				for _, msg := range streams[0].Messages {
					lastID = msg.ID
					rt.msgs = append(rt.msgs, msg.Values)
				}
			}
		}
	}()
}

func (rt *StreamTest) act() {
	rt.insertTt = &TestTable{
		ID:        1,
		FieldInt:  2,
		FieldData: []byte{1, 2, 3},
	}

	rt.mustInsert(rt.insertTt)

	rt.updateTt = &TestTable{
		ID:        1,
		FieldInt:  5,
		FieldData: []byte{4, 5, 6},
	}
	rt.mustUpdate(rt.updateTt)
}

func (rt *StreamTest) assert(t *testing.T) {
	assert.Equal(t, 2, len(rt.msgs))

	insertMsg := rt.msgs[0]
	updateMsg := rt.msgs[1]
	assert.Equal(t, rt.insertTt.AsMapString(), insertMsg)
	assert.Equal(t, rt.updateTt.AsMapString(), updateMsg)
}
