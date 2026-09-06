package pg2redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/parallelio"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2redis/stats"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeRedisPipelineClient struct {
	pipeline redis.Pipeliner
}

func (c fakeRedisPipelineClient) Pipeline() redis.Pipeliner {
	return c.pipeline
}

type erringPipeliner struct {
	redis.Pipeliner
	err error
}

func (p *erringPipeliner) Exec(context.Context) ([]redis.Cmder, error) {
	return nil, p.err
}

type noOpRedisPipelineWriter struct{}

func (noOpRedisPipelineWriter) write(context.Context, redis.Pipeliner, *pgwal.WriteEntry, time.Time, uint32) error {
	return nil
}

type failingRedisPipelineWriter struct {
	err error
}

func (w failingRedisPipelineWriter) write(context.Context, redis.Pipeliner, *pgwal.WriteEntry, time.Time, uint32) error {
	return w.err
}

func TestRedisBatchFlushCallsResponseTrackerOnError(t *testing.T) {
	ctx := context.Background()
	table := pgschema.MakeTableName("public", "redis_writer_error_test")
	errBoom := errors.New("redis boom")

	tests := []struct {
		name     string
		writer   redisPipelineWriter
		pipeline redis.Pipeliner
	}{
		{
			name:     "pipeline writer error",
			writer:   failingRedisPipelineWriter{err: errBoom},
			pipeline: &erringPipeliner{},
		},
		{
			name:     "pipeline exec error",
			writer:   noOpRedisPipelineWriter{},
			pipeline: &erringPipeliner{err: errBoom},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := newRecordingResponseTracker()
			entry := &redisBatchEntry{
				ParallelIOBatchEntry: parallelio.ParallelIOBatchEntry{
					LSN:       pglogrepl.LSN(100),
					XID:       200,
					TableName: table,
					PK:        "1",
					Offset:    3,
				},
				walEntry: &pgwal.WriteEntry{
					Table:  table,
					PK:     "1",
					Offset: 3,
				},
				commitTime: time.Now(),
			}
			buffer := &redisBatchBuffer{
				logger:      zap.NewNop(),
				redisClient: fakeRedisPipelineClient{pipeline: tt.pipeline},
				writersMap:  map[string]redisPipelineWriter{table.FullName: tt.writer},
				entries:     []*redisBatchEntry{entry},
			}

			err := buffer.flushBatch(ctx, redisFlushRequest{
				responseTracker: tracker,
				stats:           stats.NewRedisStats("test", []string{table.FullName}),
			})

			require.ErrorIs(t, err, errBoom)

			select {
			case resp := <-tracker.errors:
				require.Len(t, resp.Entries, 1)
				assert.Equal(t, entry.PK, resp.Entries[0].ResponseEntry.PK)
				assert.Equal(t, entry.TableName, resp.Entries[0].ResponseEntry.Table)
				assert.Equal(t, entry.LSN, resp.Entries[0].ResponseEntry.LSN)
				assert.Equal(t, entry.XID, resp.Entries[0].ResponseEntry.XID)
				assert.Equal(t, entry.Offset, resp.Entries[0].ResponseEntry.Offset)
				assert.EqualError(t, resp.Entries[0].Reason, errBoom.Error())
				assert.True(t, resp.Entries[0].Reason.IsRetryable())
				assert.True(t, resp.Entries[0].Reason.IsDownStreamDown())
			case resp := <-tracker.successes:
				t.Fatalf("unexpected Redis writer success acknowledgement: %+v", resp)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Redis writer returned an error but did not call ResponseTracker.OnError")
			}
		})
	}
}

func TestNewRedisWriterUsesListenerFlushSettings(t *testing.T) {
	listenerConfig := RedisListenerOptions{ListenerOpts: pg2buffer.ListenerOptions{
		AppName: "test",
	},
		WriterOpts: RedisWriterOptions{
			AppName: "test",
			FlushOpts: appconfig.FlushOptions{
				BufferSize: 11,
				QueueDepth: 7,
			},
		},
	}

	writer := newRedisWriter(
		noopTableOptionReader{},
		newRecordingResponseTracker(),
		listenerConfig.WriterOpts,
		map[string]*redisconfig.RedisTableConfig{},
		&redis.Options{Addr: "localhost:6379"},
		zap.NewNop(),
	)
	t.Cleanup(func() {
		require.NoError(t, writer.close(context.Background()))
	})

	redisWriter, ok := writer.(*redisCircuitBreakerWriter)
	require.True(t, ok)
	assert.Equal(t, 7, cap(redisWriter.eventCh))
	assert.Equal(t, uint32(11), redisWriter.flush.BufferSize)
}
