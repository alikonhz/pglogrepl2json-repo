package replicator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/appstate"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgcompatibility"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.uber.org/zap"
)

var (
	errNotSupported   = errors.New("command byte not supported")
	errUnknownMessage = errors.New("unknown message type in PG output stream")
)

var (
	Version = "0.9.2-debug"
)

// Options provide configuration options for the PGReplicator.
type Options struct {
	SlotName          string
	PubName           string
	StandByTimeout    time.Duration
	WatchXID          uint32
	WatchLSN          pglogrepl.LSN
	ReceiveMsgTimeout time.Duration
	StatsInterval     time.Duration
	MaxWriteQueueSize uint64

	standbyThreshold time.Duration
}

type state struct {
	inStream    bool
	txActive    bool
	pos         pglogrepl2json.CommitPoint
	nextStandBy time.Time
}

func (s *state) canThrottlePgMessage() bool {
	return !s.inStream && !s.txActive
}

// PGReplicator read PG logical stream and sends it to the listener (pglogrepl2json.ReplicationListener)
type PGReplicator struct {
	Options

	listener       pglogrepl2json.ReplicationListener
	statsCollector pg2stats.StatLogger
	connector      *pgconnector.PGConnector
	state          *state
	lastSentLSN    pglogrepl.LSN
	isStopping     atomic.Bool
	replicaLSN     atomic.Uint64
	logger         *zap.Logger
	cb             *circuitbreaker.CircuitBreaker

	appName   string
	syncSlots bool
	stats     *Stats
}

func NewOptions(slotName, pubName string, standByTimeout, receiveTimeout, statsInterval time.Duration, maxWriteQueueSize uint64) Options {
	var actualStandbyTimeout time.Duration
	if standByTimeout > 0 {
		actualStandbyTimeout = standByTimeout
	} else {
		actualStandbyTimeout = 10 * time.Second // default
	}

	var actualReceiveTimeout time.Duration
	if receiveTimeout > 0 {
		actualReceiveTimeout = receiveTimeout
	} else {
		// generally it should be less than the server's keep alive interval
		actualReceiveTimeout = 8 * time.Second
	}

	// if standby timeout is less than receive timeout, then set receive timeout to standby timeout minus 15% threshold
	// this is to be sure that we can send a response to the PG as soon as possible before reaching standby timeout

	const timeoutThreshold = 15
	const x100Percent = 100

	standbyThreshold := time.Duration(int64(actualStandbyTimeout) * timeoutThreshold / x100Percent)

	// receive timeout is too big, use standby timeout minus 15% threshold
	if actualReceiveTimeout >= actualStandbyTimeout {
		actualReceiveTimeout = time.Duration(int64(actualStandbyTimeout) * (x100Percent - timeoutThreshold) / x100Percent)
	}

	return Options{
		SlotName:          slotName,
		PubName:           pubName,
		MaxWriteQueueSize: maxWriteQueueSize,
		StandByTimeout:    actualStandbyTimeout,
		ReceiveMsgTimeout: actualReceiveTimeout,
		StatsInterval:     statsInterval,
		standbyThreshold:  standbyThreshold,
	}
}

// MustCreate creates new PGReplicator.
// panics when listener is nil.
func MustCreate(opts Options, listener pglogrepl2json.ReplicationListener, pgConnector *pgconnector.PGConnector, appName string) *PGReplicator {
	return MustCreateWithLogger(opts, listener, nil, pgConnector, appName, zap.NewNop())
}

func MustCreateWithLoggerAndBreakerConfig(opts Options,
	listener pglogrepl2json.ReplicationListener,
	statsCollector pg2stats.StatLogger,
	pgConnector *pgconnector.PGConnector,
	appName string,
	pgCbConfig circuitbreaker.Config,
	logger *zap.Logger) *PGReplicator {
	if listener == nil {
		panic("listener cannot be nil")
	}

	return &PGReplicator{
		Options: opts,

		listener:       listener,
		statsCollector: statsCollector,
		connector:      pgConnector,
		cb:             circuitbreaker.NewWithLogger(&pgCbConfig, logger),
		logger:         logger.Named("pg-replicator"),
		state:          &state{inStream: false},
		stats:          NewStats(appName),
		appName:        appName,
	}
}

// MustCreateWithLogger creates new PGReplicator.
// panics when the listener is nil.
func MustCreateWithLogger(opts Options,
	listener pglogrepl2json.ReplicationListener,
	statsCollector pg2stats.StatLogger,
	pgConnector *pgconnector.PGConnector,
	appName string,
	logger *zap.Logger) *PGReplicator {
	pgCbConfig := circuitbreaker.Config{
		ClosedMaxErrors:     10,
		HalfOpenedMaxErrors: 10,
		MaxErrorsInterval:   10 * time.Second,
		MaxWaitInterval:     time.Minute,
	}

	return MustCreateWithLoggerAndBreakerConfig(opts, listener, statsCollector, pgConnector, appName, pgCbConfig, logger)
}

type LSNStartReader interface {
	ReadStartLSN(ctx context.Context) (pglogrepl.LSN, error)
}

func (r *PGReplicator) GracefulShutdown(ctx context.Context) error {
	r.logger.Debug("graceful shutdown start")
	r.isStopping.Store(true)
	if err := r.listener.GracefulShutdown(ctx); err != nil {
		r.logger.Warn("graceful shutdown error", zap.Error(err))
	}

	defer func() {
		if err := r.listener.Close(context.Background()); err != nil {
			r.logger.Warn("listener stop error", zap.Error(err))
		}
	}()

	r.logger.Debug("saving state")

	commitPos := r.listener.CommitPos()

	// we save state always with background context, because we need it to succeed
	r.advanceSlotsAndSaveState(context.Background(), commitPos.LSN)

	r.logger.Debug("graceful shutdown end")

	return nil
}

func (r *PGReplicator) ReadAndStart(ctx context.Context, lsnReader LSNStartReader, doneChan chan<- error) error {
	lsn, err := lsnReader.ReadStartLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to read start LSN: %w", err)
	}

	return r.StartFromLsn(ctx, lsn, doneChan)
}

// StartFromLsn starts reading data from the PG replication connection starting from provided lsn.
// It starts a new goroutine which sends data into the provided Listener
// Cancelling ctx causes PGReplicator to stop sending data.
func (r *PGReplicator) StartFromLsn(ctx context.Context, lsn pglogrepl.LSN, doneChan chan<- error) error {
	return r.watchAndRestart(ctx, lsn, doneChan)
}

// Start starts reading data from the PG replication connection from the last saved position of the slot.
// It starts a new goroutine which sends data into the provided Listener
// Cancelling ctx causes PGReplicator to stop sending data.
func (r *PGReplicator) Start(ctx context.Context, doneChan chan<- error) error {
	return r.StartFromLsn(ctx, pglogrepl.LSN(0), doneChan)
}

func (r *PGReplicator) watchAndRestart(ctx context.Context, lsn pglogrepl.LSN, doneChan chan<- error) error {
	pluginArguments, err := r.listener.PluginArguments(r.Options.PubName)
	if err != nil {
		return fmt.Errorf("failed to get plugin arguments: %w", err)
	}

	if lsn > 0 {
		r.state.pos = pglogrepl2json.CommitPoint{
			LSN: lsn - 1, // because in standby we report lsn+1
			XID: 0,
		}
	} else {
		r.state.pos = pglogrepl2json.CommitPoint{
			LSN: pglogrepl.LSN(0),
			XID: 0,
		}
	}

	go r.printStatistics(ctx)
	go r.advanceReplSlotsLoop(ctx)
	go r.startReplication(ctx, pluginArguments, doneChan)

	return nil
}

func (r *PGReplicator) getReplConn(ctx context.Context, reconnect bool) (*pgconn.PgConn, error) {
	if reconnect {
		r.connector.Close(ctx)

		_, connErr := r.connector.ConnectPrimaryNode(ctx)

		if connErr != nil {
			return nil, connErr
		}
	}

	replConn, err := r.connector.GetPrimaryReplConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Postgres via replication connection: %w", err)
	}

	ver, err := r.connector.PrimaryVersion()
	if err != nil {
		return nil, fmt.Errorf("unable to get primary node version: %w", err)
	}

	r.logger.Debug("PG version", zap.String("pgversion", fmt.Sprintf("%d", ver)))

	return replConn, nil
}

func (r *PGReplicator) startReplication(ctx context.Context, pluginArguments []string, doneChan chan<- error) {
	replConn, err := r.getReplConn(ctx, false)

	if err != nil {
		doneChan <- err

		return
	}

replLoop:
	for {
		isStopping := r.isStopping.Load()

		if isStopping {
			err = context.Canceled
			break replLoop
		}

		if replConn == nil {
			err = r.cb.Run(ctx, func(ctx context.Context) error {
				conn, err := r.getReplConn(ctx, true)
				if err != nil {
					return err
				}

				replConn = conn

				return nil
			})
		}

		if err != nil {
			if errors.Is(err, context.Canceled) { //|| errors.Is(err, circuitbreaker.ErrOpened)
				break replLoop
			}

			r.logger.Error("failed to get replication connection", zap.Error(err))

			err = nil
			replConn = nil

			continue
		}

		err = r.cb.Run(ctx, func(ctx context.Context) error {
			return pglogrepl.StartReplication(ctx,
				replConn,
				r.Options.SlotName,
				r.state.pos.LSN,
				pglogrepl.StartReplicationOptions{
					PluginArgs: pluginArguments,
				},
			)
		})

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, circuitbreaker.ErrOpened) {
				break replLoop
			}

			r.logger.Error("failed to start replication", zap.Error(err))

			err = nil
			replConn = nil

			continue
		}

		r.logger.Debug("starting replication reader")

		runErr := r.read(ctx, replConn)

		if runErr != nil {
			replConn.Close(context.Background())
			replConn = nil

			if r.WatchXID > 0 {
				err = runErr

				break replLoop
			}

			if !isErrRecoverable(runErr) {
				if !errors.Is(runErr, context.Canceled) {
					r.logger.Error("replication reader error", zap.Error(runErr))
				}

				err = runErr

				break replLoop
			}
			// TODO:
			// think about retrying connection
			// for now the app would quit when connection is lost
			// BUG (PG2SQS):
			// 1. start app, do update on primary -> update will be sent to SQS
			// 2. restart SQS (queues will be dropped)
			// 3. simulate broken pipe (e.g. via debugger) -> i.e. write to PG fails
			// 4. app tries to reconnect and gets same write again and tries to send it
			// 5. no queue on SQS, write fails -> missing write error is raised
		}
	}

	doneChan <- err
}

func isErrRecoverable(err error) bool {
	if pgconn.Timeout(err) || errors.Is(err, net.ErrClosed) {
		return true
	}

	var netOp *net.OpError
	if errors.As(err, &netOp) {
		return true
	}

	const eof = "unexpected EOF"
	if strings.Contains(err.Error(), eof) {
		return true
	}

	return false
}

func (r *PGReplicator) printStatistics(ctx context.Context) {
	_, err := pgconnector.ExecPrimary(ctx, r.connector, func(ctx context.Context, conn *pgx.Conn) (struct{}, error) {
		return struct{}{}, pgcompatibility.CheckTrackCommitTimestamp(ctx, conn)
	})

	trackCommit := true

	if err != nil {
		trackCommit = false

		r.logger.Warn("failed to check track commit timestamp option", zap.Error(err))
	}

	t := time.NewTicker(r.StatsInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if trackCommit {
				r.findLastCommittedXid(ctx)
			}

			r.logStatistics()
		case <-ctx.Done():
			r.logger.Debug("stopped statistics")

			return
		}
	}
}

func (r *PGReplicator) findLastCommittedXid(ctx context.Context) {
	lastCommittedXid, err := findLastCommittedXid(ctx, r.connector)
	if err != nil {
		// if err is ErrNoRows it might mean there was no committed tx after server startup
		if !errors.Is(err, pgx.ErrNoRows) {
			r.logger.Warn("failed to read max commit xid", zap.Error(err))
		}

		return
	}

	r.stats.TxLastCommitted.Set(float64(lastCommittedXid))
}

func findLastCommittedXid(ctx context.Context, connector *pgconnector.PGConnector) (uint32, error) {
	return pgconnector.ExecPrimary[uint32](ctx, connector, func(ctx context.Context, conn *pgx.Conn) (uint32, error) {
		// pg_last_committed_xact returns xid,timestamp,origin
		rows, err := conn.Query(ctx, "select xid from pg_last_committed_xact() where xid is not null")
		if err != nil {
			return 0, err
		}

		return pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (uint32, error) {
			var xid uint32
			err = row.Scan(&xid)

			if err != nil {
				return 0, err
			}

			return xid, nil
		})
	})
}

func (r *PGReplicator) advanceReplSlotsLoop(ctx context.Context) {
	r.syncSlots = true

	replicasCount := r.connector.ReplicasCount()
	if replicasCount == 0 {
		r.syncSlots = false
	}

	versions := r.connector.NodeVersions()
	for node, version := range versions {
		if version < pgschema.V16 && replicasCount > 0 {
			r.logger.Sugar().Warn("node ", node, " has PG version ", version, ". replication state sync between primary and replicas supported only in PG version >= 16")

			r.syncSlots = false
		}
	}

	r.logger.Debug("starting replication state sync")

	t := time.NewTicker(r.ReceiveMsgTimeout)
	defer t.Stop()

	var lastSentLSN pglogrepl.LSN

	for {

		select {
		case <-ctx.Done():
			r.logger.Debug("stopped replication state sync")

			return

		case <-t.C:
			pointLSN := pglogrepl.LSN(r.replicaLSN.Load())
			if pointLSN > lastSentLSN {
				if ls := r.advanceSlotsAndSaveState(context.Background(), pointLSN); ls > 0 {
					lastSentLSN = ls
				}
			}
		}
	}
}

var (
	errWatchXID = errors.New("reached WatchXID")
)

func (r *PGReplicator) read(ctx context.Context, pgConn *pgconn.PgConn) error {
	r.state.nextStandBy = time.Now().Add(r.Options.StandByTimeout)

	defer pgConn.Close(context.Background())

	childCtx, cancel := context.WithCancel(ctx)

	defer cancel()

	var shutDownErr error

	go func() {
		select {
		case <-childCtx.Done():
			return
		case shutDownErr = <-r.listener.CoordinatedShutdownCh():
			cancel()

			return
		}
	}()

loop:
	for {
		if errSend := r.sendStandBy(childCtx, pgConn); errSend != nil {
			shutDownErr = errors.Join(shutDownErr, errSend)

			break loop
		}

		if shutDownErr != nil {
			break loop
		}

		queueSize := r.listener.WriteQueueSize()

		if r.Options.MaxWriteQueueSize == 0 || queueSize < r.Options.MaxWriteQueueSize || !r.state.canThrottlePgMessage() {
			if err := r.receiveAndProcessMsg(childCtx, pgConn); err != nil {
				shutDownErr = errors.Join(shutDownErr, err)
			}
		} else if shutDownErr != nil {
			r.logger.Info("write queue is full", zap.Uint64("write_queue_size", queueSize), zap.Uint64("max_write_queue_size", r.Options.MaxWriteQueueSize))
			waitTime := r.state.nextStandBy.Sub(time.Now()) - r.Options.standbyThreshold
			if waitTime > 0 {
				r.logger.Info("waiting for write queue to be empty", zap.Duration("wait_time", waitTime))
				time.Sleep(waitTime)
			}
		}

		pos := r.listener.CommitPos()

		if pos.LSN > r.state.pos.LSN {
			r.state.pos = pos
		}

		//r.logger.Debug("read loop", zap.Uint32("watchxid", r.WatchXID),
		//	zap.String("state.lsn", r.state.pos.LSN.String()),
		//	zap.Uint32("state.xid", r.state.pos.XID),
		//)
		if r.WatchXID > 0 && r.state.pos.XID >= r.WatchXID {
			r.logger.Info("got WatchXID", zap.Uint32(pglogger.XIDParam, r.WatchXID))
			shutDownErr = errors.Join(shutDownErr, fmt.Errorf("%w: %d", errWatchXID, r.WatchXID))

			break loop
		}
	}

	return shutDownErr
}

func (r *PGReplicator) receiveAndProcessMsg(ctx context.Context, pgConn *pgconn.PgConn) error {
	var rawMsg pgproto3.BackendMessage
	rawMsg, err := r.receiveMsg(ctx, pgConn)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil
		}

		return fmt.Errorf("failed to receive message: %w", err)
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		r.logger.Error("received PG wal error",
			zap.String("pgerrcode", errMsg.Code),
			zap.String("pgerrdetail", errMsg.Detail),
			zap.String("pgerrmsg", errMsg.Message),
		)

		return fmt.Errorf("received PG wal error: %s", errMsg.Message)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return fmt.Errorf("received unexpected message: %T", rawMsg)
	}

	err = r.processMsg(msg)

	if err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	return nil
}

func (r *PGReplicator) processMsg(msg *pgproto3.CopyData) error {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.processPrimaryKeepAlive(msg.Data[1:])
	case pglogrepl.XLogDataByteID:
		return r.processXLogData(msg.Data[1:])
	}

	return fmt.Errorf("%w: %d", errNotSupported, msg.Data[0])
}

func (r *PGReplicator) processXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse xlogdata: %w", err)
	}

	return r.processV2(xld.WALData)
}

func (r *PGReplicator) processV2(walData []byte) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, r.state.inStream)
	if err != nil {
		return fmt.Errorf("failed to process WAL data: %w", err)
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		return r.listener.OnRelation(logicalMsg)

	case *pglogrepl.BeginMessage:
		r.state.txActive = true
		return r.listener.OnTxBegin(logicalMsg)

	case *pglogrepl.CommitMessage:
		r.state.txActive = false
		return r.listener.OnTxCommit(logicalMsg)
	case *pglogrepl.InsertMessageV2:
		return r.listener.OnInsert(logicalMsg)
	case *pglogrepl.UpdateMessageV2:
		return r.listener.OnUpdate(logicalMsg)
	case *pglogrepl.DeleteMessageV2:
		return r.listener.OnDelete(logicalMsg)
	case *pglogrepl.TruncateMessageV2:
		return r.listener.OnTruncate(logicalMsg)
	case *pglogrepl.TypeMessageV2:
		return r.listener.OnType(logicalMsg)
	case *pglogrepl.OriginMessage:
		return r.listener.OnOrigin(logicalMsg)
	case *pglogrepl.LogicalDecodingMessageV2:
		return r.listener.OnLogicalDecodingMessage(logicalMsg)
	case *pglogrepl.StreamStartMessageV2:
		r.state.txActive = true
		r.state.inStream = true
		return r.listener.OnStreamStart(logicalMsg)
	case *pglogrepl.StreamStopMessageV2:
		r.state.inStream = false
		return r.listener.OnStreamStop(logicalMsg)
	case *pglogrepl.StreamCommitMessageV2:
		r.state.txActive = false
		return r.listener.OnStreamCommit(logicalMsg)
	case *pglogrepl.StreamAbortMessageV2:
		return r.listener.OnStreamAbort(logicalMsg)
	default:
		return fmt.Errorf("%w: %T", errUnknownMessage, logicalMsg)
	}
}

func (r *PGReplicator) processPrimaryKeepAlive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse primary keep alive message: %w", err)
	}

	if pkm.ReplyRequested {
		// reply to server as soon as possible to avoid disconnect because of timeout
		r.state.nextStandBy = time.Time{}
	}

	return nil
}

func (r *PGReplicator) receiveMsg(ctx context.Context, pgConn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
	readCtx, cancel := context.WithDeadline(ctx, time.Now().Add(r.Options.ReceiveMsgTimeout))
	rawMsg, err := pgConn.ReceiveMessage(readCtx)
	cancel()
	if err != nil {
		return nil, err
	}

	return rawMsg, nil
}

func (r *PGReplicator) sendStandBy(ctx context.Context, conn *pgconn.PgConn) error {
	now := time.Now()
	diff := r.state.nextStandBy.Sub(now)

	if r.state.nextStandBy.IsZero() ||
		now.After(r.state.nextStandBy) ||
		diff <= r.Options.standbyThreshold ||
		r.state.pos.LSN > r.lastSentLSN ||
		r.isStopping.Load() {
		lsnPos := r.state.pos.LSN + 1

		err := pglogrepl.SendStandbyStatusUpdate(ctx,
			conn,
			pglogrepl.StandbyStatusUpdate{
				WALWritePosition: lsnPos,
			},
		)

		if err != nil {
			return err
		}

		r.stats.TxLastReported.Set(float64(r.state.pos.XID))
		r.stats.LSNLastReported.Set(float64(lsnPos))

		//r.logger.Debug("standby sent", zap.Uint32(pglogger.XIDParam, r.state.pos.XID), zap.String(pglogger.LSNParam, lsnPos.String()))

		r.state.nextStandBy = time.Now().Add(r.Options.StandByTimeout)

		r.lastSentLSN = r.state.pos.LSN
		r.replicaLSN.Store(uint64(lsnPos))
	}

	return nil
}

func (r *PGReplicator) GetStatsAsFields() []zap.Field {
	fields := []zap.Field{
		zap.String("version", Version),
		zap.Uint32(r.stats.TxLastCommitted.Name(), uint32(r.stats.TxLastCommitted.Value())),
		zap.Uint32(r.stats.TxLastReported.Name(), uint32(r.stats.TxLastReported.Value())),
		zap.String(r.stats.LSNLastReported.Name(), pglogrepl.LSN(r.stats.LSNLastReported.Value()).String()),
	}

	if r.statsCollector == nil {
		return fields
	}

	return append(fields, r.statsCollector.GetStatsAsFields()...)
}

func (r *PGReplicator) logStatistics() {
	statFields := r.GetStatsAsFields()

	r.logger.Info("statistics", statFields...)
}

func (r *PGReplicator) advanceSlotsAndSaveState(ctx context.Context, pointLSN pglogrepl.LSN) pglogrepl.LSN {
	r.logger.Debug("syncing replication slot status", zap.String(pglogger.LSNParam, pointLSN.String()))

	if uint64(pointLSN) == 0 {
		return 0
	}

	err := appstate.Save(ctx, r.connector, &appstate.AppState{
		AppName: r.appName,
		State:   pointLSN.String(),
		LastXID: "0",
	})

	if err != nil {
		r.logger.Warn("appstate: failed to save state", zap.Error(err))
	}

	_ = r.saveListenerState(ctx, pointLSN, err)

	if r.syncSlots {
		_, err = pgconnector.ExecReplica(ctx, r.connector, func(ctx context.Context, conn *pgx.Conn) (struct{}, error) {
			_, execErr := conn.Exec(ctx, "select pg_replication_slot_advance($1, $2)", r.SlotName, pointLSN)

			return struct{}{}, execErr
		})

		if err != nil {
			const (
				invalidSlotStateCode = "55000"
				minimumMsg           = "minimum"
			)

			var pgErr *pgconn.PgError

			// we might get an error "cannot advance replication slot to <lsn>, minimum is <lsn>"
			// we should ignore this error
			if !errors.As(err, &pgErr) || pgErr.Code != invalidSlotStateCode || strings.Index(pgErr.Message, minimumMsg) < 0 {
				r.logger.Error("failed to sync replication slot status", zap.Error(err), zap.String(pglogger.LSNParam, pointLSN.String()))
			}

			return 0
		}
	}

	return pointLSN
}

func (r *PGReplicator) saveListenerState(ctx context.Context, pointLSN pglogrepl.LSN, err error) error {
	err = r.listener.SaveState(ctx, pointLSN)
	if err != nil {
		r.logger.Warn("listener: failed to save state", zap.Error(err))
	}

	return err
}
