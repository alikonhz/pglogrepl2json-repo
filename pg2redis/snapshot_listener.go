package pg2redis

import (
	"context"
	"fmt"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisSnapshotListener struct {
	redisWriter RedisWriter
	tables      map[string]*pgschema.Table
	appName     string
	txOpts      appconfig.TxCommitTimeOptions
	buffered    *pg2buffer.PGBufferedListener
}

type redisSnapshotWriterFactory func(optReader TableOptionReader,
	responseTracker pg2buffer.ResponseTracker,
	writerConfig RedisWriterOptions,
	tablesCfg map[string]*redisconfig.RedisTableConfig,
	redisOpts *redis.Options,
	logger *zap.Logger) RedisWriter

func NewRedisSnapshotListener(listenerConfig RedisListenerOptions,
	redisOpts *redis.Options,
	pgConnector *pgconnector.PGConnector,
	tablesConfig map[string]*config.TableConfig,
	redisTables map[string]*redisconfig.RedisTableConfig,
	tables map[string]*pgschema.Table,
	retryConfig circuitbreaker.RetryConfig,
	logger *zap.Logger) *RedisSnapshotListener {

	return newRedisSnapshotListener(listenerConfig,
		redisOpts,
		pgConnector,
		tablesConfig,
		redisTables,
		tables,
		retryConfig,
		logger,
		newRedisWriter)
}

func newRedisSnapshotListener(listenerConfig RedisListenerOptions,
	redisOpts *redis.Options,
	pgConnector *pgconnector.PGConnector,
	tablesConfig map[string]*config.TableConfig,
	redisTables map[string]*redisconfig.RedisTableConfig,
	tables map[string]*pgschema.Table,
	retryConfig circuitbreaker.RetryConfig,
	logger *zap.Logger,
	writerFactory redisSnapshotWriterFactory) *RedisSnapshotListener {

	l := &RedisSnapshotListener{
		tables:  tables,
		appName: listenerConfig.WriterOpts.AppName,
		txOpts:  listenerConfig.WriterOpts.TxTimeOpts,
	}
	bufferedWriter := &redisSnapshotBufferedWriter{
		appName: listenerConfig.WriterOpts.AppName,
	}

	// We use PGBufferedListener to get queue, back-pressure, and retry logic for free.
	// We also create a dedicated Redis writer instance so snapshot acknowledgements
	// are reported through the snapshot listener's own response tracker.
	l.buffered = pg2buffer.NewListener(bufferedWriter, listenerConfig.ListenerOpts, pgConnector, tablesConfig, retryConfig, logger)
	l.redisWriter = writerFactory(l.buffered, l.buffered.ResponseTracker(), listenerConfig.WriterOpts, redisTables, redisOpts, logger)
	bufferedWriter.redisWriter = l.redisWriter
	bufferedWriter.txOpts = l.txOpts
	l.buffered.Start(context.Background())
	l.redisWriter.start(context.Background())

	return l
}

func (l *RedisSnapshotListener) OnSnapshotBatch(ctx context.Context, tableName pgschema.TableName, batch *pgwal.WriteRequest) error {
	return l.buffered.OnSnapshotBatch(ctx, tableName, batch)
}

func (l *RedisSnapshotListener) TxOptions() appconfig.TxCommitTimeOptions {
	return l.txOpts
}

func (l *RedisSnapshotListener) SetSnapshotAckHandler(handler func(*pgwal.Response, pglogrepl2json.CommitPoint)) {
	l.buffered.SetSnapshotAckHandler(handler)
}

func (l *RedisSnapshotListener) GetTable(tableName pgschema.TableName) *pgschema.Table {
	return l.tables[tableName.String()]
}

func (l *RedisSnapshotListener) CommitPos() pglogrepl2json.CommitPoint {
	return l.buffered.CommitPos()
}

func (l *RedisSnapshotListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return l.redisWriter.SaveSnapshotLSN(ctx, snapshotLSNKey(l.appName), lsn)
}

func (l *RedisSnapshotListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	return l.redisWriter.GetSnapshotLSN(ctx, snapshotLSNKey(l.appName))
}

func (l *RedisSnapshotListener) GracefulShutdown(ctx context.Context) error {
	return l.buffered.GracefulShutdown(ctx)
}

func (l *RedisSnapshotListener) Close(ctx context.Context) error {
	return l.redisWriter.close(ctx)
}

func (l *RedisSnapshotListener) WriteQueueSize() uint64 {
	return l.buffered.WriteQueueSize()
}

type redisSnapshotBufferedWriter struct {
	redisWriter RedisWriter
	appName     string
	txOpts      appconfig.TxCommitTimeOptions
}

func (w *redisSnapshotBufferedWriter) TxOptions() appconfig.TxCommitTimeOptions {
	return w.txOpts
}

func (w *redisSnapshotBufferedWriter) Write(ctx context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) {
	_ = w.redisWriter.enqueue(ctx, req, responseTracker)
}

func (w *redisSnapshotBufferedWriter) Ping(ctx context.Context) error {
	return w.redisWriter.ping(ctx)
}

func (w *redisSnapshotBufferedWriter) GracefulShutdown(ctx context.Context) error {
	return w.redisWriter.gracefulShutdown()
}

func (w *redisSnapshotBufferedWriter) SaveState(ctx context.Context, lsn pglogrepl.LSN) error {
	return nil // Not used for snapshots
}

func (w *redisSnapshotBufferedWriter) Close(ctx context.Context) error {
	return w.redisWriter.close(ctx)
}

func (w *redisSnapshotBufferedWriter) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return w.redisWriter.SaveSnapshotLSN(ctx, snapshotLSNKey(w.appName), lsn)
}

func (w *redisSnapshotBufferedWriter) GetSnapshotLSN(ctx context.Context) (string, error) {
	return w.redisWriter.GetSnapshotLSN(ctx, snapshotLSNKey(w.appName))
}

func snapshotLSNKey(appName string) string {
	return fmt.Sprintf("pgwalk:snapshot_lsn:%s", appName)
}
