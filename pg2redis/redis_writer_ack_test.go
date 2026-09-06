package pg2redis

import (
	"context"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRedisWriterAcknowledgesSuccessfulFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	appConf, err := redisconfig.LoadAppConfig("")
	require.NoError(t, err)

	redisOpts, err := appConf.ReadRedisOptions(3 * time.Second)
	require.NoError(t, err)

	redisClient := redis.NewClient(redisOpts)
	t.Cleanup(func() {
		require.NoError(t, redisClient.Close())
	})
	require.NoError(t, redisClient.Ping(ctx).Err())

	table := pgschema.MakeTableName("public", "redis_writer_ack_test")
	key := pgschema.MakeRowKey(table.FullName, "1")
	require.NoError(t, redisClient.Del(ctx, key).Err())

	tablesCfg := map[string]*redisconfig.RedisTableConfig{
		table.FullName: {
			Commands: []any{[]string{"HSET", "{%schema%}.{%table%}:{%pk%}", "{pairs:*}"}},
			FullName: table.FullName,
		},
	}
	testAppConfig := &redisconfig.RedisAppConfig{Tables: tablesCfg}
	redisconfig.NewFromConfig(testAppConfig).WellForm(nil)

	tracker := newRecordingResponseTracker()
	listenerConfig := RedisListenerOptions{
		ListenerOpts: pg2buffer.ListenerOptions{},
		WriterOpts: RedisWriterOptions{
			AppName: string(licensemanager.ProductPG2REDIS),
			FlushOpts: appconfig.FlushOptions{
				Interval:   time.Hour,
				BufferSize: 1,
			},
		}}

	writer := newRedisWriter(noopTableOptionReader{}, tracker, listenerConfig.WriterOpts, testAppConfig.Tables, redisOpts, zap.NewNop())
	writer.start(ctx)
	t.Cleanup(func() {
		require.NoError(t, writer.gracefulShutdown())
		require.NoError(t, writer.close(context.Background()))
	})

	req := &pgwal.WriteRequest{
		LSN:        pglogrepl.LSN(100),
		XID:        200,
		CommitTime: time.Now(),
		Entries: []*pgwal.WriteEntry{
			{
				Table:  table,
				PK:     "1",
				Kind:   pgwal.Insert,
				Offset: 0,
				Tuple: func() *orderedmap.OrderedMap {
					tuple := newTestOrderedMap("id", "field_int")
					tuple.Set("id", 1)
					tuple.Set("field_int", 42)
					return tuple
				}(),
			},
		},
	}

	require.NoError(t, writer.enqueue(ctx, req, tracker))

	require.Eventually(t, func() bool {
		fields, err := redisClient.HGetAll(ctx, key).Result()
		return err == nil && fields["field_int"] == "42"
	}, 3*time.Second, 20*time.Millisecond, "Redis write should be flushed before checking ack")

	select {
	case errResp := <-tracker.errors:
		t.Fatalf("unexpected Redis writer error acknowledgement: %+v", errResp)
	case resp := <-tracker.successes:
		require.Len(t, resp.Entries, 1)
		assert.Equal(t, req.LSN, resp.Entries[0].LSN)
		assert.Equal(t, req.XID, resp.Entries[0].XID)
		assert.Equal(t, req.Entries[0].Offset, resp.Entries[0].Offset)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Redis writer flushed the batch to Redis but did not call ResponseTracker.OnSuccess")
	}
}

type noopTableOptionReader struct{}

func (noopTableOptionReader) GetTableOption(_ pgschema.TableName, _ string) (string, bool) {
	return "", false
}

type recordingResponseTracker struct {
	successes chan *pgwal.Response
	errors    chan *pgwal.ErrResponse
}

func newRecordingResponseTracker() *recordingResponseTracker {
	return &recordingResponseTracker{
		successes: make(chan *pgwal.Response, 1),
		errors:    make(chan *pgwal.ErrResponse, 1),
	}
}

func (t *recordingResponseTracker) OnSuccess(resp *pgwal.Response) pglogrepl2json.CommitPoint {
	t.successes <- resp
	return pglogrepl2json.CommitPoint{}
}

func (t *recordingResponseTracker) OnError(resp *pgwal.ErrResponse) {
	t.errors <- resp
}
