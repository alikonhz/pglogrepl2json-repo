package pg2redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRedisSnapshotListenerGracefulShutdownStopsWriterOnce(t *testing.T) {
	writer := &recordingRedisWriter{}
	listenerConfig := RedisListenerOptions{
		ListenerOpts: pg2buffer.ListenerOptions{
			AppName: "test_app",
		},
		WriterOpts: RedisWriterOptions{
			FlushOpts: appconfig.FlushOptions{
				Workers: 1,
			},
		},
	}

	listener := newRedisSnapshotListener(listenerConfig,
		&redis.Options{},
		nil,
		map[string]*config.TableConfig{},
		map[string]*redisconfig.RedisTableConfig{},
		nil,
		circuitbreaker.RetryConfig{},
		zap.NewNop(),
		func(TableOptionReader,
			pg2buffer.ResponseTracker,
			RedisWriterOptions,
			map[string]*redisconfig.RedisTableConfig,
			*redis.Options,
			*zap.Logger) RedisWriter {
			return writer
		})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, listener.GracefulShutdown(ctx))
	assert.Equal(t, int32(1), writer.shutdownCalls.Load())
}

type recordingRedisWriter struct {
	shutdownCalls atomic.Int32
}

func (w *recordingRedisWriter) enqueue(ctx context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) error {
	return nil
}

func (w *recordingRedisWriter) ping(ctx context.Context) error {
	return nil
}

func (w *recordingRedisWriter) gracefulShutdown() error {
	w.shutdownCalls.Add(1)
	return nil
}

func (w *recordingRedisWriter) readLSN(ctx context.Context) (string, error) {
	return "", nil
}

func (w *recordingRedisWriter) start(ctx context.Context) {
}

func (w *recordingRedisWriter) saveLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	return nil
}

func (w *recordingRedisWriter) close(ctx context.Context) error {
	return nil
}

func (w *recordingRedisWriter) GetStatsAsFields() []zap.Field {
	return nil
}

func (w *recordingRedisWriter) SaveSnapshotLSN(ctx context.Context, key string, lsn string) error {
	return nil
}

func (w *recordingRedisWriter) GetSnapshotLSN(ctx context.Context, key string) (string, error) {
	return "", nil
}
