package pg2redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/parallelio"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/condition"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2redis/stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type TableOptionReader interface {
	GetTableOption(table pgschema.TableName, option string) (string, bool)
}

func newRedisWriter(optReader TableOptionReader,
	responseTracker pg2buffer.ResponseTracker,
	writerOpts RedisWriterOptions,
	tablesCfg map[string]*redisconfig.RedisTableConfig,
	redisOpts *redis.Options,
	logger *zap.Logger) RedisWriter {

	tableNames := make([]string, 0, len(tablesCfg))
	for tableName := range tablesCfg {
		tableNames = append(tableNames, tableName)
	}
	redisStats := stats.NewRedisStats(writerOpts.AppName, tableNames)

	rw := redisWriter{
		logger: logger.Named("writer-redis"),
		txOpts: writerOpts.TxTimeOpts,
		stats:  redisStats,
	}

	writersMap := make(map[string]redisPipelineWriter)
	for tableName, config := range tablesCfg {
		w := createPipelineWriter(config, rw, optReader)
		writersMap[tableName] = w
	}

	return &redisCircuitBreakerWriter{
		writersMap:      writersMap,
		optReader:       optReader,
		client:          redis.NewClient(redisOpts),
		redisWriter:     rw,
		responseTracker: responseTracker,
		cb:              circuitbreaker.NewWithLogger(circuitbreaker.NewProdConfig(), logger),
		eventCh:         make(chan any, writerOpts.FlushOpts.QueueDepth),
		closeCh:         make(chan struct{}),
		wg:              &sync.WaitGroup{},
		flush:           writerOpts.FlushOpts,
		stats:           redisStats,
	}
}

func createPipelineWriter(cfg *redisconfig.RedisTableConfig, rw redisWriter, optReader TableOptionReader) redisPipelineWriter {
	return &redisDynamicWriter{
		redisWriter:           rw,
		cfg:                   cfg,
		evaluator:             condition.NewEvaluator(),
		resolvedCommands:      mustCompileCommandGroups(cfg.ResolvedCommands, rw.logger),
		insertCommands:        mustCompileCommandGroups(cfg.InsertCommands, rw.logger),
		updateCommands:        mustCompileCommandGroups(cfg.UpdateCommands, rw.logger),
		deleteCommands:        mustCompileCommandGroups(cfg.DeleteCommands, rw.logger),
		defaultDeleteCommands: mustCompileCommandGroups(defaultDeleteCommands(cfg.FullName), rw.logger),
	}
}

type redisPipelineWriter interface {
	write(ctx context.Context, pp redis.Pipeliner, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32) error
}

type redisCircuitBreakerWriter struct {
	redisWriter
	optReader  TableOptionReader
	client     *redis.Client
	cb         *circuitbreaker.CircuitBreaker
	writersMap map[string]redisPipelineWriter

	responseTracker pg2buffer.ResponseTracker

	closeCh    chan struct{}
	parallelIO *parallelio.ParallelIO
	flush      appconfig.FlushOptions

	wg      *sync.WaitGroup
	eventCh chan any
	stats   *stats.RedisStats
}

type redisPipelineClient interface {
	Pipeline() redis.Pipeliner
}

func (rcb *redisCircuitBreakerWriter) GetStatsAsFields() []zap.Field {
	return rcb.stats.GetStatsAsFields()
}

func (rcb *redisCircuitBreakerWriter) gracefulShutdown() error {
	close(rcb.closeCh)

	if rcb.parallelIO != nil {
		rcb.parallelIO.Close()
	}

	rcb.logger.Info("waiting for Redis writer to stop")
	rcb.wg.Wait()
	rcb.logger.Info("Redis writer stopped")

	return nil
}

func (rcb *redisCircuitBreakerWriter) start(ctx context.Context) {
	rcb.wg.Add(1)
	go rcb.runWorker(ctx)
}

func (rcb *redisCircuitBreakerWriter) enqueue(_ context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) error {
	if len(req.Entries) == 0 {
		responseTracker.OnSuccess(&pgwal.Response{Entries: []*pgwal.ResponseEntry{
			{
				LSN: req.LSN,
				XID: req.XID,
			},
		}})

		return nil
	}

	for _, entry := range req.Entries {
		batchEntry := &redisBatchEntry{
			ParallelIOBatchEntry: parallelio.ParallelIOBatchEntry{
				LSN:       req.LSN,
				XID:       req.XID,
				TableName: entry.Table,
				PK:        entry.PK,
				Offset:    entry.Offset,
			},
			walEntry:   entry,
			commitTime: req.CommitTime,
		}

		rcb.eventCh <- batchEntry
	}

	return nil
}

type redisBatchEntry struct {
	parallelio.ParallelIOBatchEntry
	walEntry   *pgwal.WriteEntry
	commitTime time.Time
}

type redisFlushRequest struct {
	workerID        uint32
	requestID       uint64
	responseTracker pg2buffer.ResponseTracker
	stats           *stats.RedisStats
	flushTimeout    time.Duration
}

func (rcb *redisCircuitBreakerWriter) ioHandler(ctx context.Context, request parallelio.TrackedIORequest) error {
	buffer, _ := request.Request.(*redisBatchBuffer)

	return buffer.flushBatch(ctx, redisFlushRequest{
		workerID:        request.WorkerID,
		requestID:       request.RequestID,
		responseTracker: rcb.responseTracker,
		stats:           rcb.stats,
		flushTimeout:    rcb.flush.Timeout,
	})
}

func (rcb *redisCircuitBreakerWriter) runWorker(ctx context.Context) {
	rcb.parallelIO = parallelio.NewParallelIO(ctx, rcb.ioHandler, rcb.flush.Workers, rcb.logger)
	defer rcb.wg.Done()

	newBatchBuffer := func() *redisBatchBuffer {
		return &redisBatchBuffer{
			logger:      rcb.logger,
			redisClient: rcb.client,
			writersMap:  rcb.writersMap,
			entries:     make([]*redisBatchEntry, 0),
		}
	}

	batchBuffer := newBatchBuffer()

	tryFlushBatch := func(bySize bool) error {
		if batchBuffer.isEmpty() {
			return nil
		}

		if bySize {
			rcb.stats.BatchesFlushSizeTotal.Inc()
		} else {
			rcb.stats.BatchesFlushTimerTotal.Inc()
		}

		buffer := batchBuffer
		batchBuffer = newBatchBuffer()

		return rcb.parallelIO.Submit(buffer)
	}

	tickerDuration := rcb.flush.Interval
	if tickerDuration == 0 {
		tickerDuration = 500 * time.Millisecond
	}

	ticker := time.NewTicker(tickerDuration)
	isTimerRunning := false

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rcb.closeCh:
			return
		case req := <-rcb.eventCh:
			switch r := req.(type) {
			case *redisBatchEntry:
				if batchBuffer == nil {
					batchBuffer = newBatchBuffer()
				}

				if !isTimerRunning {
					isTimerRunning = true

					ticker.Reset(tickerDuration)
				}

				rcb.logger.Debug("redis worker: append entry to batch", zap.String(pglogger.LSNParam, r.LSN.String()),
					zap.Uint32(pglogger.XIDParam, r.XID),
					zap.String(pglogger.TableParam, r.TableName.String()),
					zap.String(pglogger.PKParam, r.PK),
				)
				batchBuffer.append(r)

				if rcb.flush.BufferSize > 0 && batchBuffer.length() >= rcb.flush.BufferSize {
					rcb.logger.Debug("flushing by size: start")

					if err := tryFlushBatch(true); err != nil {
						rcb.logger.Error("failed to flush", zap.Error(err))
						panic("handle flush by size error")
						//TODO: rcb.handleError(err)
					}

					rcb.logger.Debug("flushing by size: end")
				}
			default:
				panic("unexpected request type:")
				//TODO: rcb.handleError(fmt.Errorf("%w: %v", ErrUnexpectedRequestType, r))
			}
		case <-ticker.C:
			isTimerRunning = false

			ticker.Stop()
			rcb.logger.Debug("flushing by timer: start")

			if err := tryFlushBatch(false); err != nil {
				panic("handle flush by timer error")
				//rcb.handleError(err)
			}

			rcb.logger.Debug("flushing all queues by timer: end")
		}
	}
}

type redisBatchBuffer struct {
	entries     []*redisBatchEntry
	logger      *zap.Logger
	redisClient redisPipelineClient
	writersMap  map[string]redisPipelineWriter
}

func (rb *redisBatchBuffer) append(entry *redisBatchEntry) {
	rb.entries = append(rb.entries, entry)
}

func (rb *redisBatchBuffer) isEmpty() bool {
	return len(rb.entries) == 0
}

func (rb *redisBatchBuffer) length() uint32 {
	return uint32(len(rb.entries))
}

type redisFlushError struct {
	reason error
}

func (e *redisFlushError) Error() string {
	return e.reason.Error()
}

func (e *redisFlushError) IsRetryable() bool {
	return !isNonRetryableRedisError(e.reason)
}

func (e *redisFlushError) IsDownStreamDown() bool {
	return !isNonRetryableRedisError(e.reason)
}

func isNonRetryableRedisError(err error) bool {
	var redisErr redis.Error
	if !errors.As(err, &redisErr) {
		return false
	}

	msg := strings.ToUpper(redisErr.Error())
	return strings.HasPrefix(msg, "WRONGTYPE") ||
		strings.HasPrefix(msg, "ERR UNKNOWN COMMAND") ||
		strings.HasPrefix(msg, "ERR SYNTAX ERROR")
}

func (rb *redisBatchBuffer) notifyError(req redisFlushRequest, err error) {
	if req.responseTracker == nil || len(rb.entries) == 0 {
		return
	}

	errEntries := make([]*pgwal.ErrorResponseEntry, len(rb.entries))
	for i, failedEntry := range rb.entries {
		errEntries[i] = &pgwal.ErrorResponseEntry{
			ResponseEntry: pgwal.ResponseEntry{
				PK:     failedEntry.PK,
				Table:  failedEntry.TableName,
				LSN:    failedEntry.LSN,
				XID:    failedEntry.XID,
				Offset: failedEntry.Offset,
			},
			Reason: &redisFlushError{reason: err},
		}
	}

	req.responseTracker.OnError(&pgwal.ErrResponse{
		Entries: errEntries,
	})
}

func (rb *redisBatchBuffer) flushBatch(ctx context.Context, req redisFlushRequest) error {
	pp := rb.redisClient.Pipeline()

	req.stats.BatchesInflight.Add(1)
	defer req.stats.BatchesInflight.Add(-1)

	var maxXID uint32
	var lastLSN pglogrepl.LSN

	for _, entry := range rb.entries {
		var (
			ppWriter redisPipelineWriter
			ok       bool
		)

		if ppWriter, ok = rb.writersMap[entry.walEntry.Table.FullName]; !ok {
			// IMPORTANT: DO NOT remove this code. When the app encounters a table missing from the config, it should log and panic
			rb.logger.Fatal("cannot find writer for table", zap.String("table", entry.walEntry.Table.Name))
			// this line should be unreachable
			panic(fmt.Errorf("cannot find writer for table %s", entry.walEntry.Table.Name))
		}

		if entry.XID > maxXID {
			maxXID = entry.XID
		}
		if entry.LSN > lastLSN {
			lastLSN = entry.LSN
		}

		if err := ppWriter.write(ctx, pp, entry.walEntry, entry.commitTime, entry.XID); err != nil {
			rb.notifyError(req, err)
			return err
		}
	}

	req.stats.TxLastSent.Set(float64(maxXID))
	req.stats.BatchSize.Set(float64(len(rb.entries)))

	t1 := time.Now()
	execCtx := ctx
	var cancel context.CancelFunc
	if req.flushTimeout > 0 {
		execCtx, cancel = context.WithTimeout(ctx, req.flushTimeout)
		defer cancel()
	}

	rb.logger.Debug("executing pipeline: start")
	_, err := pp.Exec(execCtx)
	rb.logger.Debug("executing pipeline: end")
	req.stats.BatchDurationSeconds.UpdateDuration(t1)

	if err != nil {
		req.stats.TrackFailure("pipeline_exec", "pipeline", "multiple")
		rb.notifyError(req, err)
		return err
	}

	req.stats.TxLastOK.Set(float64(maxXID))
	req.stats.LSNLastFlushed.Set(float64(lastLSN))

	okEntries := make([]*pgwal.ResponseEntry, len(rb.entries))
	for i, entry := range rb.entries {
		okEntries[i] = &pgwal.ResponseEntry{
			PK:     entry.PK,
			Table:  entry.TableName,
			LSN:    entry.LSN,
			XID:    entry.XID,
			Offset: entry.Offset,
		}
	}

	req.responseTracker.OnSuccess(&pgwal.Response{Entries: okEntries})

	// Update lag if possible
	if len(rb.entries) > 0 {
		lastEntry := rb.entries[len(rb.entries)-1]
		if !lastEntry.commitTime.IsZero() {
			req.stats.LagSeconds.Set(time.Since(lastEntry.commitTime).Seconds())
		}
	}

	// Update batches total for each table involved
	tables := make(map[string]struct{})
	for _, entry := range rb.entries {
		tables[entry.walEntry.Table.FullName] = struct{}{}
	}
	for table := range tables {
		req.stats.BatchesTotal.Add(table, 1)
	}

	return nil
}

func (rcb *redisCircuitBreakerWriter) ping(ctx context.Context) error {
	_, err := rcb.client.Ping(ctx).Result()
	return err
}

func (rcb *redisCircuitBreakerWriter) readLSN(ctx context.Context) (string, error) {
	res, err := rcb.client.Get(ctx, redisLSNKey).Result()
	if err != nil {
		return "", fmt.Errorf("readLSN error: %w", err)
	}

	return res, nil
}

func (rcb *redisCircuitBreakerWriter) saveLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	if lsn > 0 {
		rcb.stats.LSNLastReported.Set(float64(lsn))
		return rcb.client.Set(ctx, redisLSNKey, lsn.String(), 0).Err()
	}

	return nil
}

func (rcb *redisCircuitBreakerWriter) SaveSnapshotLSN(ctx context.Context, key string, lsn string) error {
	return rcb.client.Set(ctx, key, lsn, 0).Err()
}

func (rcb *redisCircuitBreakerWriter) GetSnapshotLSN(ctx context.Context, key string) (string, error) {
	val, err := rcb.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

func (rcb *redisCircuitBreakerWriter) close(_ context.Context) error {
	return rcb.client.Close()
}

type redisDynamicWriter struct {
	redisWriter
	cfg                   *redisconfig.RedisTableConfig
	evaluator             *condition.Evaluator
	resolvedCommands      []redisCompiledCommandGroup
	insertCommands        []redisCompiledCommandGroup
	updateCommands        []redisCompiledCommandGroup
	deleteCommands        []redisCompiledCommandGroup
	defaultDeleteCommands []redisCompiledCommandGroup
}

type redisCommand struct {
	name  string
	args  []any
	table string
}

type redisCompiledCommandConfig struct {
	name string
	args []redisCompiledCommand
}

type redisCompiledCommandGroup struct {
	condition *redisconfig.ConditionConfig
	commands  []redisCompiledCommandConfig
}

func defaultDeleteCommands(tableName string) []redisconfig.RedisCommandWithCondition {
	return []redisconfig.RedisCommandWithCondition{{
		Commands: [][]string{{"DEL", fmt.Sprintf("%s:%%pk%%", tableName)}},
	}}
}

func mustCompileCommandGroups(groups []redisconfig.RedisCommandWithCondition, logger *zap.Logger) []redisCompiledCommandGroup {
	compiledGroups := make([]redisCompiledCommandGroup, 0, len(groups))
	for _, group := range groups {
		compiledGroup := redisCompiledCommandGroup{
			condition: group.Condition,
			commands:  make([]redisCompiledCommandConfig, 0, len(group.Commands)),
		}

		for _, cmd := range group.Commands {
			compiled, ok, err := compileCommandConfig(cmd)
			if err != nil {
				logger.Fatal("failed to compile Redis command", zap.Error(err), zap.Any("command", cmd))
			}
			if !ok {
				continue
			}

			compiledGroup.commands = append(compiledGroup.commands, compiled)
		}

		if len(compiledGroup.commands) > 0 {
			compiledGroups = append(compiledGroups, compiledGroup)
		}
	}

	return compiledGroups
}

func compileCommandConfig(cmdCfg redisconfig.RedisCommandConfig) (redisCompiledCommandConfig, bool, error) {
	if len(cmdCfg) == 0 {
		return redisCompiledCommandConfig{}, false, nil
	}

	cmd := redisCompiledCommandConfig{
		name: strings.ToUpper(cmdCfg[0]),
		args: make([]redisCompiledCommand, 0, len(cmdCfg)-1),
	}

	for i := 1; i < len(cmdCfg); i++ {
		compiled, err := compileCommand(cmdCfg[i])
		if err != nil {
			return redisCompiledCommandConfig{}, false, err
		}

		cmd.args = append(cmd.args, compiled)
	}

	return cmd, true, nil
}

func (r *redisDynamicWriter) write(ctx context.Context, pp redis.Pipeliner, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32) error {
	var opType redisconfig.OperationType
	switch entry.Kind {
	case pgwal.Insert, pgwal.SnapshotRead:
		opType = redisconfig.OpInsert
	case pgwal.Update:
		opType = redisconfig.OpUpdate
	case pgwal.Delete:
		opType = redisconfig.OpDelete
	default:
		return nil
	}

	commands := r.getCommands(opType)
	if len(commands) == 0 {
		r.logger.Fatal("no commands configured", zap.String("table", entry.Table.FullName))
		panic(fmt.Errorf("no commands configured for table %s", entry.Table.FullName))
	}

	resolvedCommands := make([]redisCommand, 0, len(commands))
	for _, cmdWithCond := range commands {
		if cmdWithCond.condition != nil {
			match, err := r.evaluator.Evaluate(cmdWithCond.condition, entry)
			if err != nil {
				r.logger.Error("failed to evaluate condition", zap.Error(err), zap.String("table", entry.Table.FullName))
				continue
			}
			if !match {
				continue
			}
		}

		for _, cmd := range cmdWithCond.commands {
			resolved, ok, err := r.buildCommand(cmd, entry, commitTime, xid)
			if err != nil {
				return fmt.Errorf("failed to build Redis command %s for table %s: %w", cmd.name, entry.Table.FullName, err)
			}
			if !ok {
				continue
			}
			resolvedCommands = append(resolvedCommands, resolved)
		}
	}

	// we wrap commands inside MULTI/EXEC only if len(resolvedCommands) > 1
	if len(resolvedCommands) > 1 {
		if err := r.enqueueTransaction(ctx, pp, resolvedCommands, entry.Table.FullName); err != nil {
			return err
		}

		return nil
	}

	// if there's only one command, we execute it directly
	if len(resolvedCommands) == 1 {
		return r.enqueueCommand(ctx, pp, resolvedCommands[0])
	}

	return nil
}

func (r *redisDynamicWriter) enqueueTransaction(ctx context.Context, pp redis.Pipeliner, commands []redisCommand, tableName string) error {
	r.logger.Debug("execute Redis transaction", zap.String("table", tableName), zap.Int("commands", len(commands)))

	if err := pp.Do(ctx, "MULTI").Err(); err != nil {
		r.stats.TrackFailure(fmt.Sprintf("%T", err), "MULTI", tableName)
		return err
	}

	for _, cmd := range commands {
		if err := r.enqueueCommand(ctx, pp, cmd); err != nil {
			return err
		}
	}

	if err := pp.Do(ctx, "EXEC").Err(); err != nil {
		r.stats.TrackFailure(fmt.Sprintf("%T", err), "EXEC", tableName)
		return err
	}

	return nil
}

func (r *redisDynamicWriter) buildCommand(cmdCfg redisCompiledCommandConfig, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32) (redisCommand, bool, error) {
	if cmdCfg.name == "" {
		return redisCommand{}, false, nil
	}

	requiredLen := uint32(1)
	for _, cmdArg := range cmdCfg.args {
		argLen, err := cmdArg(true, nil, 0, entry, commitTime, xid, r.txOpts)
		if err != nil {
			return redisCommand{}, false, err
		}
		requiredLen += argLen
	}

	args := make([]any, requiredLen)
	args[0] = cmdCfg.name

	next := uint32(1)
	for _, cmdArg := range cmdCfg.args {
		var err error
		next, err = cmdArg(false, args, next, entry, commitTime, xid, r.txOpts)
		if err != nil {
			return redisCommand{}, false, err
		}
	}

	return redisCommand{
		name:  cmdCfg.name,
		args:  args[:next],
		table: entry.Table.FullName,
	}, true, nil
}

func (r *redisDynamicWriter) enqueueCommand(ctx context.Context, pp redis.Pipeliner, cmdConfig redisCommand) error {
	if dbg := r.logger.Check(zap.DebugLevel, "execute Redis command"); dbg != nil {
		dbg.Write(zap.String("command", cmdConfig.name), zap.Int("arg_count", len(cmdConfig.args)-1))
	}

	r.stats.TrackCommand(cmdConfig.name, cmdConfig.table)

	cmd := pp.Do(ctx, cmdConfig.args...)
	err := cmd.Err()
	if err != nil {
		r.stats.TrackFailure(fmt.Sprintf("%T", err), cmdConfig.name, cmdConfig.table)

		return err
	}

	return nil
}

func (r *redisDynamicWriter) getCommands(opType redisconfig.OperationType) []redisCompiledCommandGroup {
	var ops []redisCompiledCommandGroup
	switch opType {
	case redisconfig.OpInsert:
		ops = r.insertCommands
	case redisconfig.OpUpdate:
		ops = r.updateCommands
	case redisconfig.OpDelete:
		ops = r.deleteCommands
	}

	if len(ops) > 0 {
		return ops
	}

	// Default behavior for DELETE when no specific delete commands provided:
	if opType == redisconfig.OpDelete {
		return r.defaultDeleteCommands
	}

	return r.resolvedCommands
}

type redisWriter struct {
	logger *zap.Logger
	txOpts appconfig.TxCommitTimeOptions
	stats  *stats.RedisStats
}

func convertToFloat64(v any) (float64, bool, error) {
	if v == nil {
		return 0, false, nil
	}

	switch val := v.(type) {
	case decode.SmartFloat32:
		return float64(val), true, nil
	case decode.SmartFloat64:
		return float64(val), true, nil
	case float64:
		return val, true, nil
	case float32:
		return float64(val), true, nil
	case int64:
		return float64(val), true, nil
	case uint64:
		return float64(val), true, nil
	case int32:
		return float64(val), true, nil
	case uint32:
		return float64(val), true, nil
	case int16:
		return float64(val), true, nil
	case uint16:
		return float64(val), true, nil
	case int8:
		return float64(val), true, nil
	case uint8:
		return float64(val), true, nil
	case int:
		return float64(val), true, nil
	case uint:
		return float64(val), true, nil
	case pgtype.Numeric:
		f, err := val.Float64Value()
		if err != nil {
			return 0, false, err
		}
		return f.Float64, true, nil
	}

	return 0, false, fmt.Errorf("convertToFloat64: unsupported type %T", v)
}
