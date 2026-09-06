package pg2sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/appstate"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/listeners"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2runner"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsconfig"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pglogrepl"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
)

type SQSListenerOptions struct {
	ListenerOpts pg2buffer.ListenerOptions

	WriterOpts SQSWriterOptions
}

type SQSWriterOptions struct {
	TxTimeOpts appconfig.TxCommitTimeOptions
	FlushOpts  appconfig.FlushOptions
}

func NewListenerConfig(appConfig *sqsconfig.SQSAppConfig) (*SQSListenerOptions, error) {
	lc := pg2buffer.NewListenerOptions(appConfig.Postgres.Repl.Pub,
		appConfig.Postgres.NumericMode,
		string(licensemanager.ProductPG2SQS),
	)

	txOpts := appconfig.NewTxCommitTimeOptions(appConfig.SQS.CommitTimeColumn)
	flushOpts, err := appconfig.NewFlushOptions(appConfig.SQSFlushInterval,
		10, // SQS max, it doesn't support other values
		appConfig.SQSFlushQueueDepth,
		appConfig.SQSFlushWorkers,
		appConfig.SQSWriteTimeout)

	if err != nil {
		return nil, err
	}

	sqsOpts := &SQSListenerOptions{
		ListenerOpts: lc,
		WriterOpts: SQSWriterOptions{
			TxTimeOpts: txOpts,
			FlushOpts:  flushOpts,
		},
	}

	return sqsOpts, nil
}

func NewListener(sqsClient *sqs.Client,
	listenerConfig *SQSListenerOptions,
	pgConnector *pgconnector.PGConnector,
	appConf *sqsconfig.SQSAppConfig,
	logger *zap.Logger) *SQSListener {
	sqsListener := &SQSListener{} //nolint

	retryConfig, _ := appConf.RetryPolicy().AsRetryConfig()

	ls := pg2buffer.NewListener(sqsListener, listenerConfig.ListenerOpts, pgConnector, appConf.TablesConfig(), retryConfig, logger)

	sqsListener.listener = ls
	sqsListener.sqsWriter = newSqsWriter(sqsClient, ls.ResponseTracker(), appConf.Tables, listenerConfig, logger)
	sqsListener.pgConnector = pgConnector
	sqsListener.listenerConfig = listenerConfig
	sqsListener.logger = logger.Named("listener-sqs")

	return sqsListener
}

type SQSListener struct {
	listener       *pg2buffer.PGBufferedListener
	listenerConfig *SQSListenerOptions
	sqsWriter      *sqsWriter
	pgConnector    *pgconnector.PGConnector

	logger *zap.Logger
}

var (
	_ = pg2buffer.PGWALWriter((*SQSListener)(nil))
	_ = pg2runner.ListenerBuilder((*SQSListener)(nil))
)

type SQSError struct {
	reason error
}

func (e *SQSError) Error() string {
	return e.reason.Error()
}

func (e *SQSError) IsRetryable() bool {
	return true
}

func (e *SQSError) IsDownStreamDown() bool {
	return errors.Is(e.reason, circuitbreaker.ErrOpened)
}

func (sqsLis *SQSListener) GracefulShutdown(ctx context.Context) error {
	sqsLis.sqsWriter.GracefulShutdown(ctx)

	return nil
}

func (sqsLis *SQSListener) Close(ctx context.Context) error {
	return nil
}

func (sqsLis *SQSListener) SaveState(_ context.Context, _ pglogrepl.LSN) error {
	return nil
}

func (sqsLis *SQSListener) TxOptions() appconfig.TxCommitTimeOptions {
	return sqsLis.listenerConfig.WriterOpts.TxTimeOpts
}

func (sqsLis *SQSListener) Write(ctx context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) {
	if ctx.Err() != nil {
		sqsLis.logger.Debug("write cancelled", zap.Uint32(pglogger.XIDParam, req.XID), zap.String(pglogger.LSNParam, req.LSN.String()))
		return
	}

	sqsLis.logger.Debug("write start", zap.Uint32(pglogger.XIDParam, req.XID), zap.String(pglogger.LSNParam, req.LSN.String()))

	err := sqsLis.sqsWriter.enqueueOrWrite(ctx, req, responseTracker)

	if err != nil {
		sqsLis.logger.Error("failed to send to SQS",
			zap.Uint32(pglogger.XIDParam, req.XID),
			zap.String(pglogger.LSNParam, req.LSN.String()),
			zap.Error(err),
		)

		respEntries := make([]*pgwal.ErrorResponseEntry, len(req.Entries))
		for i := 0; i < len(req.Entries); i++ {
			respEntries[i] = &pgwal.ErrorResponseEntry{
				ResponseEntry: pgwal.ResponseEntry{
					PK:     req.Entries[i].PK,
					Table:  req.Entries[i].Table,
					LSN:    req.LSN,
					XID:    req.XID,
					Offset: req.Entries[i].Offset,
				},
				Reason: &SQSError{reason: err},
			}
		}

		responseTracker.OnError(&pgwal.ErrResponse{
			Entries: respEntries,
		})

		return
	}

	sqsLis.logger.Debug("write end", zap.Uint32(pglogger.XIDParam, req.XID), zap.String(pglogger.LSNParam, req.LSN.String()))
}

func (sqsLis *SQSListener) Ping(ctx context.Context) error {
	err := sqsLis.sqsWriter.ping(ctx)
	if err != nil {
		return fmt.Errorf("unable to connect to SQS: %w", err)
	}

	return nil
}

func (sqsLis *SQSListener) LoadPubTables(ctx context.Context) error {
	return sqsLis.listener.LoadPubTables(ctx) //nolint:wrapcheck
}

func (sqsLis *SQSListener) ValidateConfig(ctx context.Context) error {
	return sqsLis.sqsWriter.ValidateConfig(ctx, sqsLis.listener.GetPubTableColumns())
}

func (sqsLis *SQSListener) Start(ctx context.Context) {
	sqsLis.listener.Start(ctx)
	sqsLis.sqsWriter.Start(ctx)
}

func (sqsLis *SQSListener) close() error {
	return sqsLis.listener.GracefulShutdown(context.Background())
}

func (sqsLis *SQSListener) Listener() pglogrepl2json.ReplicationListener {
	return sqsLis.listener
}

func (sqsLis *SQSListener) SnapshotListener(_ int) pglogrepl2json.SnapshotListener {
	return listeners.NewNoSnapshotListener()
}

func (sqsLis *SQSListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return nil
}

func (sqsLis *SQSListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	return "", nil
}

func (sqsLis *SQSListener) StatsCollector() pg2stats.StatLogger {
	return sqsLis
}

func (sqsLis *SQSListener) GetLastLSN(ctx context.Context, connector *pgconnector.PGConnector) (string, error) {
	state, err := appstate.Load(ctx, connector, "pg2sqs")

	if err != nil {
		return "", err
	}

	return state.State, nil
}

func (sqsLis *SQSListener) GetStatsAsFields() []zap.Field {
	return append(sqsLis.listener.GetStatsAsFields(),
		sqsLis.sqsWriter.GetStatsAsFields()...,
	)
}
