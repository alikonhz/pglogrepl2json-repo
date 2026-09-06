package pg2redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	redisLSNKey                 = "pg2redis_lsn"
	defaultRedisFlushBufferSize = 100
)

type RedisListenerOptions struct {
	ListenerOpts pg2buffer.ListenerOptions

	WriterOpts RedisWriterOptions
}

type RedisWriterOptions struct {
	AppName string

	TxTimeOpts appconfig.TxCommitTimeOptions
	FlushOpts  appconfig.FlushOptions
}

func NewListenerOptions(appConfig *redisconfig.RedisAppConfig) (*RedisListenerOptions, error) {
	lc := pg2buffer.NewListenerOptions(appConfig.Postgres.Repl.Pub,
		appConfig.Postgres.NumericMode,
		string(licensemanager.ProductPG2REDIS))

	txOpts := appconfig.NewTxCommitTimeOptions(appConfig.Redis.CommitTimeColumn)
	flushOpts, err := appconfig.NewFlushOptions(appConfig.RedisFlushInterval,
		appConfig.RedisFlushBufferSize,
		appConfig.RedisFlushQueueDepth,
		appConfig.RedisFlushWorkers,
		appConfig.RedisWriteTimeout)

	if err != nil {
		return nil, err
	}

	rlc := &RedisListenerOptions{
		ListenerOpts: lc,
		WriterOpts: RedisWriterOptions{
			AppName:    lc.AppName,
			TxTimeOpts: txOpts,
			FlushOpts:  flushOpts,
		},
	}

	if rlc.WriterOpts.FlushOpts.BufferSize == 0 {
		rlc.WriterOpts.FlushOpts.BufferSize = defaultRedisFlushBufferSize
	}

	return rlc, nil
}

type RedisWriter interface {
	enqueue(ctx context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) error
	ping(ctx context.Context) error
	gracefulShutdown() error
	readLSN(ctx context.Context) (string, error)
	start(ctx context.Context)
	saveLSN(ctx context.Context, lsn pglogrepl.LSN) error
	close(ctx context.Context) error
	GetStatsAsFields() []zap.Field
	SaveSnapshotLSN(ctx context.Context, key string, lsn string) error
	GetSnapshotLSN(ctx context.Context, key string) (string, error)
}

type RedisListener struct {
	listener       *pg2buffer.PGBufferedListener
	listenerConfig RedisListenerOptions
	redisWriter    RedisWriter
	redisOpts      *redis.Options
	pgConnector    *pgconnector.PGConnector
	appConfig      *redisconfig.RedisAppConfig
	retryConfig    circuitbreaker.RetryConfig
	logger         *zap.Logger
}

func newListener(redisOpts *redis.Options,
	listenerConfig RedisListenerOptions,
	pgConnector *pgconnector.PGConnector,
	appConfig *redisconfig.RedisAppConfig,
	logger *zap.Logger,
	alterWriter func(w RedisWriter) RedisWriter) *RedisListener {

	rls := &RedisListener{}

	retryConfig, _ := appConfig.RetryPolicy().AsRetryConfig()

	ls := pg2buffer.NewListener(rls, listenerConfig.ListenerOpts, pgConnector, appConfig.TablesConfig(), retryConfig, logger)
	writer := newRedisWriter(ls, ls.ResponseTracker(), listenerConfig.WriterOpts, appConfig.Tables, redisOpts, logger)
	if alterWriter != nil {
		writer = alterWriter(writer)
	}

	rls.listener = ls
	rls.redisWriter = writer
	rls.redisOpts = redisOpts
	rls.pgConnector = pgConnector
	rls.listenerConfig = listenerConfig
	rls.appConfig = appConfig
	rls.retryConfig = retryConfig
	rls.logger = logger.Named("listener-redis")

	return rls
}

func (rl *RedisListener) TxOptions() appconfig.TxCommitTimeOptions {
	return rl.listenerConfig.WriterOpts.TxTimeOpts
}

func (rl *RedisListener) GracefulShutdown(_ context.Context) error {
	return rl.redisWriter.gracefulShutdown()
}

func (rl *RedisListener) Close(ctx context.Context) error {
	return rl.redisWriter.close(ctx)
}

func (rl *RedisListener) SaveState(ctx context.Context, lsn pglogrepl.LSN) error {
	return rl.redisWriter.saveLSN(ctx, lsn)
}

func (rl *RedisListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	key := fmt.Sprintf("pgwalk:snapshot_lsn:%s", rl.listenerConfig.WriterOpts.AppName)
	return rl.redisWriter.SaveSnapshotLSN(ctx, key, lsn)
}

func (rl *RedisListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	key := fmt.Sprintf("pgwalk:snapshot_lsn:%s", rl.listenerConfig.WriterOpts.AppName)
	return rl.redisWriter.GetSnapshotLSN(ctx, key)
}

func (rl *RedisListener) Listener() pglogrepl2json.ReplicationListener {
	return rl.listener
}

func (rl *RedisListener) SnapshotListener(workers int) pglogrepl2json.SnapshotListener {
	tables := make(map[string]*pgschema.Table)
	for name, t := range rl.listener.GetPubTables() {
		tables[name] = t
	}

	// for the snapshot we use parallel workers = Redis flush workers
	lc := rl.listenerConfig
	if workers > 0 {
		lc.WriterOpts.FlushOpts.Workers = uint32(workers)
	}

	return NewRedisSnapshotListener(lc,
		rl.redisOpts,
		rl.pgConnector,
		rl.appConfig.TablesConfig(),
		rl.appConfig.Tables,
		tables,
		rl.retryConfig,
		rl.logger)
}

func NewListener(redisOpts *redis.Options,
	listenerOpts RedisListenerOptions,
	pgConnector *pgconnector.PGConnector,
	appConfig *redisconfig.RedisAppConfig,
	logger *zap.Logger) *RedisListener {

	return newListener(redisOpts, listenerOpts, pgConnector, appConfig, logger, func(w RedisWriter) RedisWriter {
		return w
	})
}

func (rl *RedisListener) GetStatsAsFields() []zap.Field {
	return append(rl.listener.GetStatsAsFields(),
		rl.redisWriter.GetStatsAsFields()...,
	)
}

func (rl *RedisListener) StatsCollector() pg2stats.StatLogger {
	return rl
}

func (rl *RedisListener) WriterType() string {
	return fmt.Sprintf("%T", rl.redisWriter)
}

func (rl *RedisListener) Ping(ctx context.Context) error {
	err := rl.redisWriter.ping(ctx)
	if err != nil {
		return fmt.Errorf("unable to connect to redis: %w", err)
	}

	return nil
}

func (rl *RedisListener) Write(ctx context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) {
	for {
		if ctx.Err() != nil {
			break
		}

		rl.logger.Debug("enqueueing redis write", zap.Uint32(pglogger.XIDParam, req.XID), zap.String(pglogger.LSNParam, req.LSN.String()))

		err := rl.redisWriter.enqueue(context.Background(), req, responseTracker)
		if err != nil {
			if _, ok := errors.AsType[redis.Error](err); ok {
				if strings.HasPrefix(err.Error(), "WRONGTYPE") {
					// TODO: documentation
					// TODO: setting -> NoTxSkip
					rl.logger.Warn("an attempt was made to write the wrong type into Redis. transaction will be skipped",
						zap.Uint32(pglogger.XIDParam, req.XID),
						zap.String(pglogger.LSNParam, req.LSN.String()),
						zap.Error(err),
					)
				}
			} else {
				rl.logger.Error("failed to send to redis",
					zap.Uint32(pglogger.XIDParam, req.XID),
					zap.String(pglogger.LSNParam, req.LSN.String()),
					zap.Error(err),
				)
				continue
			}
		}

		rl.logger.Debug("enqueueing redis write done", zap.Uint32(pglogger.XIDParam, req.XID), zap.String(pglogger.LSNParam, req.LSN.String()))

		break
	}
}

func (rl *RedisListener) LoadPubTables(ctx context.Context) error {
	return rl.listener.LoadPubTables(ctx)
}

func (rl *RedisListener) ValidateConfig(_ context.Context) error {
	return rl.appConfig.ValidateAgainstSchema(rl.listener.GetPubTables())
}

func (rl *RedisListener) Start(ctx context.Context) {
	rl.listener.Start(ctx)
	rl.redisWriter.start(ctx)
}

func (rl *RedisListener) GetLastLSN(ctx context.Context, _ *pgconnector.PGConnector) (string, error) {
	return rl.redisWriter.readLSN(context.Background())
}
