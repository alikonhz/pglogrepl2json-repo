package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/entrypool"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/replicationslot"
	"github.com/alikonhz/pglogrepl2json/writequeue"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type SnapshotManager struct {
	config        *appconfig.SnapshotConfig
	connector     *pgconnector.PGConnector
	stateManager  snapshotStateStore
	queryBuilder  *QueryBuilder
	listener      pglogrepl2json.SnapshotListener
	appName       string
	logger        *zap.Logger
	slotSnapshot  *replicationslot.Snapshot
	statsInterval time.Duration
}

type snapshotStateStore interface {
	CreateSnapshotStateTableIfNotExists(ctx context.Context) error
	LoadTablesState(ctx context.Context) (map[string]*TableState, error)
	UpsertTableState(ctx context.Context, state *TableState) error
	UpdateProgress(ctx context.Context, tableName string, processedRows int64, status SnapshotStatus, errorMessage *string) error
	SetInProgress(ctx context.Context, tableName string, totalRows int64) error
}

type snapshotStateStoreCloser interface {
	Close(ctx context.Context) error
}

func NewManager(config *appconfig.SnapshotConfig, connector *pgconnector.PGConnector, listener pglogrepl2json.SnapshotListener, appName string, logger *zap.Logger) *SnapshotManager {
	return &SnapshotManager{
		config:        config,
		connector:     connector,
		stateManager:  NewStateManager(connector, appName, logger),
		queryBuilder:  NewQueryBuilder(),
		listener:      listener,
		appName:       appName,
		logger:        logger.Named("snapshot-manager"),
		statsInterval: time.Minute,
	}
}

func (m *SnapshotManager) WithSlotSnapshot(slotSnapshot *replicationslot.Snapshot) *SnapshotManager {
	m.slotSnapshot = slotSnapshot
	return m
}

func (m *SnapshotManager) WithStatsInterval(statsInterval time.Duration) *SnapshotManager {
	m.statsInterval = statsInterval
	return m
}

func (m *SnapshotManager) Execute(ctx context.Context) (pglogrepl.LSN, error) {
	if m.config == nil || m.config.Mode == appconfig.ModeNever {
		return 0, nil
	}

	if err := m.config.Validate(); err != nil {
		return 0, fmt.Errorf("invalid snapshot config: %w", err)
	}

	// Check if we already have a snapshot LSN
	lsnStr, err := m.listener.GetSnapshotLSN(ctx)
	if err != nil {
		m.logger.Warn("failed to get snapshot LSN from", zap.Error(err))
	}

	if lsnStr != "" {
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err == nil && lsn > 0 && m.config.Mode == appconfig.ModeOneTime {
			m.logger.Info("snapshot already completed", zap.String("lsn", lsnStr))

			// If mode is OneTime, we don't re-run.
			return lsn, nil
		}
	}

	defer m.closeStateManager()

	err = m.stateManager.CreateSnapshotStateTableIfNotExists(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to initialize state manager: %w", err)
	}

	lsn, snapshotID, cleanup, err := m.prepareSnapshot(ctx)
	if err != nil {
		return 0, err
	}

	commitSnapshot := false

	defer func() {
		cleanup(commitSnapshot)
	}()

	// we need to shut down with a separate context
	defer m.listener.GracefulShutdown(context.Background())

	parsedLSN, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return 0, fmt.Errorf("failed to parse snapshot LSN: %w", err)
	}

	tableStates, err := m.initTablesState(ctx, lsn)
	if err != nil {
		return 0, err
	}

	err = m.runSnapshot(ctx, tableStates, snapshotID)
	if err != nil {
		return parsedLSN, err
	}

	commitSnapshot = true

	// Save completed snapshot LSN
	err = m.listener.SaveSnapshotLSN(context.Background(), lsn)
	if err != nil {
		m.logger.Error("failed to save snapshot LSN", zap.Error(err))
	}

	return parsedLSN, nil
}

func (m *SnapshotManager) closeStateManager() {
	closer, ok := m.stateManager.(snapshotStateStoreCloser)
	if !ok {
		return
	}

	if err := closer.Close(context.Background()); err != nil {
		m.logger.Warn("failed to close snapshot state manager", zap.Error(err))
	}
}

func (m *SnapshotManager) prepareSnapshot(ctx context.Context) (string, string, func(bool), error) {
	if m.slotSnapshot != nil && m.slotSnapshot.LSN > 0 && m.slotSnapshot.SnapshotName != "" {
		lsn := m.slotSnapshot.LSN.String()
		m.logger.Info("using replication slot snapshot",
			zap.String("lsn", lsn),
			zap.String("snapshot_id", m.slotSnapshot.SnapshotName))

		return lsn, m.slotSnapshot.SnapshotName, func(bool) {}, nil
	}

	conn, err := m.connector.AcquirePrimary(ctx)
	if err != nil {
		return "", "", nil, fmt.Errorf("failed to acquire primary connection: %w", err)
	}

	cleanup := func(commit bool) {
		command := "ROLLBACK"
		if commit {
			command = "COMMIT"
		}

		_, _ = conn.Exec(context.Background(), command)
		_ = conn.Close(context.Background())
	}

	var lsn, snapshotID string
	_, err = conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
	if err != nil {
		cleanup(false)
		return "", "", nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	err = conn.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID)
	if err != nil {
		cleanup(false)
		return "", "", nil, fmt.Errorf("failed to export snapshot: %w", err)
	}

	err = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	if err != nil {
		cleanup(false)
		return "", "", nil, fmt.Errorf("failed to get current WAL LSN: %w", err)
	}

	m.logger.Info("exported snapshot", zap.String("lsn", lsn), zap.String("snapshot_id", snapshotID))

	return lsn, snapshotID, cleanup, nil
}

func (m *SnapshotManager) initTablesState(ctx context.Context, snapshotLSN string) ([]*TableState, error) {
	existingStates, err := m.stateManager.LoadTablesState(ctx)
	if err != nil {
		return nil, err
	}

	var tableStates []*TableState

	for _, tableCfg := range m.config.Config {
		tableName := tableCfg.Name
		state, shouldUpsert := m.prepareTableState(tableName, tableCfg, existingStates[tableName], snapshotLSN)

		if shouldUpsert {
			err = m.stateManager.UpsertTableState(ctx, state)
			if err != nil {
				return nil, err
			}
		}

		tableStates = append(tableStates, state)
	}

	return tableStates, nil
}

func (m *SnapshotManager) prepareTableState(tableName string, tableCfg *appconfig.SnapshotTableConfig, state *TableState, snapshotLSN string) (*TableState, bool) {
	if state != nil && state.Status == StatusCompleted {
		return state, false
	}

	if state == nil {
		state = &TableState{
			TableName: tableName,
			AppName:   m.appName,
		}
	}

	state.whereClause = ""
	if tableCfg.Type == appconfig.SnapshotTableQuery {
		state.whereClause = " ( " + strings.TrimSpace(tableCfg.Query) + " ) "
	}

	snapshotQuery := m.queryBuilder.BuildSelectQuery(tableName, m.snapshotQueryColumns(tableName), state.whereClause)

	state.SnapshotLSN = snapshotLSN
	state.SnapshotType = string(tableCfg.Type)
	state.SnapshotQuery = &snapshotQuery
	state.TotalRows = nil
	state.ProcessedRows = 0
	state.Status = StatusPending
	state.ErrorMessage = nil

	return state, true
}

type snapshotTask struct {
	state  *TableState
	table  *pgschema.Table
	keyMap *keymap.KeyMap
	query  string
	args   []any
}

type progressReportKind string

const (
	reportKindMaxLSN          = "max_lsn"
	reportKindTaskDone        = "task_done"
	reportKindRangeDone       = "range_done"
	reportKindLSNAck          = "lsn_ack"
	reportKindTableStarted    = "table_start"
	reportKindTableDispatched = "table_done"
)

type progressReport struct {
	kind progressReportKind

	tableName     string
	rowsProcessed int64
	totalRows     int64

	maxLSN     pglogrepl.LSN
	tasksCount int
}

func (r progressReport) Size() uint32 {
	return 1
}

type snapshotProgressReporter struct {
	mu     sync.Mutex
	ch     chan progressReport
	queue  *writequeue.WriteQueue[progressReport]
	closed bool
}

func newSnapshotProgressReporterAndDispatch(bufferSize int, logger *zap.Logger) *snapshotProgressReporter {
	reporter := &snapshotProgressReporter{
		ch:    make(chan progressReport, bufferSize),
		queue: writequeue.New[progressReport](logger),
	}

	go reporter.dispatch(context.Background())

	return reporter
}

func newSnapshotProgressReporter(bufferSize int, logger *zap.Logger) *snapshotProgressReporter {
	reporter := &snapshotProgressReporter{
		ch:    make(chan progressReport, bufferSize),
		queue: writequeue.New[progressReport](logger),
	}

	return reporter
}

func (r *snapshotProgressReporter) Reports() <-chan progressReport {
	return r.ch
}

func (r *snapshotProgressReporter) Report(ctx context.Context, progress progressReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.queue.Push(progress)

	return nil
}

func (r *snapshotProgressReporter) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}

	r.closed = true
	r.queue.Push(progressReport{tableName: "", kind: "stop"})
	r.mu.Unlock()
}

func (r *snapshotProgressReporter) dispatch(ctx context.Context) {
	defer close(r.ch)

	for {
		progress, stopped := r.queue.Pop()
		if stopped || progress.kind == "stop" || ctx.Err() != nil {
			return
		}

		r.ch <- progress
	}
}

type snapshotTableProgress struct {
	processedRows            int64
	processedSinceLastReport int64
	tableState               *TableState

	dispatched bool
	tasksCount int
	doneTasks  int
	totalRows  int64
	maxLSN     pglogrepl.LSN
}

func (table *snapshotTableProgress) isDone(maxAck pglogrepl.LSN) bool {
	return table.doneTasks == table.tasksCount && table.dispatched && table.maxLSN <= maxAck &&
		table.maxLSN > 0
}

type tableEstimate struct {
	estimatedRows     int64
	baseEstimatedRows int64
	totalBlocks       int64
}

type tableError struct {
	tableName string
	err       error
}

type snapshotTablePlan struct {
	query          string
	ranges         []ctidRange
	usesCtidRanges bool
	strategy       string
	blocksPerBatch int64
}

type ctidBounds struct {
	min pgtype.TID
	max pgtype.TID
}

const (
	querySnapshotSparseSelectivityPercent = 10
	querySnapshotSparseMaxBatches         = 10
)

func (m *SnapshotManager) setTableCompleted(ctx context.Context, tableName string, processedRows, totalRows int64) {
	err := m.stateManager.UpdateProgress(ctx, tableName, processedRows, StatusCompleted, nil)
	if err != nil {
		m.logger.Error("failed to mark snapshot table completed", zap.String("table", tableName), zap.Error(err))
		return
	}

	fields := []zap.Field{
		zap.String("table", tableName),
		zap.Int64("processed_rows", processedRows),
	}
	if totalRows > 0 {
		fields = append(fields, zap.Int64("total_rows", totalRows))
	}

	m.logger.Info("snapshot table marked as completed", fields...)
}

func (m *SnapshotManager) runSnapshot(ctx context.Context, tableStates []*TableState, snapshotID string) error {
	maxWorkers := m.config.ParallelWorkers
	if maxWorkers <= 0 {
		maxWorkers = 1
	}

	lsnTracker := &snapshotLSNTracker{}
	taskCh := make(chan snapshotTask)

	progressReporter := newSnapshotProgressReporter(100, m.logger)
	go progressReporter.dispatch(ctx)

	progressDoneCh := make(chan struct{})
	m.listener.SetSnapshotAckHandler(func(resp *pgwal.Response, maxAcked pglogrepl2json.CommitPoint) {
		m.reportSnapshotAckProgress(ctx, progressReporter, resp, maxAcked)
	})
	defer m.listener.SetSnapshotAckHandler(nil)

	go m.trackProgress(ctx, tableStates, progressDoneCh, progressReporter)

	errChan := make(chan tableError, maxWorkers*2) // Some buffer for errors
	allErrors := make(map[string][]error)
	errDoneCh := make(chan struct{})
	pool := entrypool.NewPool()

	go func() {
		defer close(errDoneCh)
		for err := range errChan {
			if !errors.Is(err.err, context.Canceled) {
				allErrors[err.tableName] = append(allErrors[err.tableName], err.err)
			}
		}
	}()

	var wg sync.WaitGroup

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker := NewWorker(id, m.connector, m.listener, m.config.BatchSize, snapshotID, lsnTracker, m.listener.TxOptions(), pool, m.logger)
			worker.Process(ctx, id, taskCh, errChan, progressReporter)
		}(i)
	}

	go m.runSnapshotLoop(ctx, tableStates, snapshotID, taskCh, errChan, maxWorkers, progressReporter)

	wg.Wait()

	if err := m.waitForSnapshotAcks(ctx, lsnTracker.MaxSubmitted()); err != nil {
		progressReporter.Close()
		<-progressDoneCh
		close(errChan)
		<-errDoneCh
		return err
	}

	progressReporter.Close()
	<-progressDoneCh

	close(errChan)
	<-errDoneCh

	findTableState := func(tableName string) *TableState {
		for _, st := range tableStates {
			if st.TableName == tableName {
				return st
			}
		}

		return nil
	}

	if len(allErrors) > 0 {
		var failedTables []string
		for tableName, tableErrors := range allErrors {
			failedTables = append(failedTables, tableName)
			var errStr []string

			for _, err := range tableErrors {
				errStr = append(errStr, err.Error())
			}

			errorMsg := strings.Join(errStr, "\n")
			st := findTableState(tableName)
			if st != nil {
				st.Status = StatusFailed
			}

			_ = m.stateManager.UpdateProgress(context.Background(), tableName, 0, StatusFailed, &errorMsg)
		}

		if m.config.AbortOnError {
			return fmt.Errorf("snapshot finished with errors for table(s): %s. see error details in the pgwalk.snapshot_state table",
				strings.Join(failedTables, ", "))
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// from now on we're going to use context.Background()
	// we need to make sure that any finished tables will be marked as completed to prevent them from being re-run

	for _, st := range tableStates {
		if st.Status != StatusCompleted && st.Status != StatusFailed {
			_ = m.stateManager.UpdateProgress(context.Background(), st.TableName, st.ProcessedRows, StatusCompleted, nil)
			fields := []zap.Field{
				zap.String("table", st.TableName),
				zap.Int64("processed_rows", st.ProcessedRows),
			}
			if st.TotalRows != nil {
				fields = append(fields, zap.Int64("total_rows", *st.TotalRows))
			}

			m.logger.Info("snapshot completed", fields...)
		}
	}

	return nil
}

func (m *SnapshotManager) runSnapshotLoop(ctx context.Context, tableStates []*TableState, snapshotID string, taskCh chan snapshotTask, errChan chan tableError, maxWorkers int, progressReporter *snapshotProgressReporter) {
	defer close(taskCh)
	for _, st := range tableStates {
		if st.Status == StatusCompleted {
			continue
		}

		if ctx.Err() != nil {
			return
		}

		table := m.listener.GetTable(pgschema.ParseTableName(st.TableName))
		if table == nil {
			m.logger.Error("table not found in listener", zap.String("table", st.TableName))
			errChan <- tableError{tableName: st.TableName, err: fmt.Errorf("table %s not found in listener", st.TableName)}
			continue
		}

		selectColumns := snapshotColumnNames(table)
		if len(selectColumns) == 0 {
			errChan <- tableError{tableName: st.TableName, err: fmt.Errorf("table %s has no snapshot columns", st.TableName)}
			return
		}

		snapshotQuery := m.queryBuilder.BuildSelectQuery(st.TableName, snapshotQueryColumns(table), st.whereClause)
		st.SnapshotQuery = &snapshotQuery
		keyMap := snapshotKeyMap(selectColumns, m.listener.TxOptions())

		estimate, err := m.getTableEstimate(ctx, st)

		if err != nil {
			m.logger.Warn("failed to estimate table density", zap.String("table", st.TableName), zap.Error(err))
			errChan <- tableError{tableName: st.TableName, err: err}
			// abort on any error
			return
		}

		// Initialize table as started if it's pending
		if st.Status == StatusPending {
			if err := m.stateManager.SetInProgress(ctx, st.TableName, estimate.estimatedRows); err != nil {
				errChan <- tableError{tableName: st.TableName, err: err}
				return
			}

			m.logger.Info("snapshot table started",
				zap.String("table", st.TableName),
				zap.Int64("estimated_rows", estimate.estimatedRows),
			)
		}

		plan, err := m.planSnapshotTable(ctx, st, estimate, maxWorkers, snapshotID)
		if err != nil {
			m.logger.Warn("failed to plan snapshot table", zap.String("table", st.TableName), zap.Error(err))
			errChan <- tableError{tableName: st.TableName, err: err}
			return
		}

		m.logger.Info("planned snapshot table",
			zap.String("table", st.TableName),
			zap.String("strategy", plan.strategy),
			zap.Int64("estimated_rows", estimate.estimatedRows),
			zap.Int64("base_estimated_rows", estimate.baseEstimatedRows),
		)

		err = progressReporter.Report(ctx, progressReport{tableName: st.TableName, kind: reportKindTableStarted, totalRows: estimate.estimatedRows})
		if err != nil {
			m.logger.Error("failed to report table start", zap.String("table", st.TableName), zap.Error(err))
			errChan <- tableError{tableName: st.TableName, err: fmt.Errorf("failed to report table start: %w", err)}
			return
		}

		tasksCount := 0
		if plan.usesCtidRanges && len(plan.ranges) > 0 {
			canThrottle := m.config.MaxWriteQueueSize > 0
			for _, r := range plan.ranges {
				qs := m.listener.WriteQueueSize()
				if canThrottle && qs > m.config.MaxWriteQueueSize {
					var totalWaitTime time.Duration
					sleepTime := 10 * time.Millisecond

					for {
						time.Sleep(sleepTime)
						totalWaitTime += sleepTime
						//if totalWaitTime > time.Minute {
						//	m.logger.Fatal("write queue hasn't been drained within one minute")
						//}

						if m.listener.WriteQueueSize() <= m.config.MaxWriteQueueSize {
							break
						}
					}
				}

				taskCh <- snapshotTask{
					state:  st,
					table:  table,
					keyMap: keyMap,
					query:  plan.query,
					args:   []any{r.min, r.max},
				}

				tasksCount++
			}
		} else if !plan.usesCtidRanges && plan.query != "" {
			taskCh <- snapshotTask{
				state:  st,
				table:  table,
				keyMap: keyMap,
				query:  plan.query,
			}

			tasksCount++
		}

		err = progressReporter.Report(ctx, progressReport{tableName: st.TableName, kind: reportKindTableDispatched, tasksCount: tasksCount})
		if err != nil {
			m.logger.Error("failed to report table dispatched", zap.String("table", st.TableName), zap.Error(err))
			errChan <- tableError{tableName: st.TableName, err: fmt.Errorf("failed to report table dispatched: %w", err)}
			return
		}
	}
}

func (m *SnapshotManager) trackProgress(ctx context.Context, tableStates []*TableState, progressDoneCh chan struct{}, progressReporter *snapshotProgressReporter) {
	defer close(progressDoneCh)
	progressTracker := newSnapshotProgressTracker(tableStates, 10)
	statsCtx, stopStats := context.WithCancel(ctx)
	defer stopStats()
	go m.printStatistics(statsCtx, progressTracker)

	for progress := range progressReporter.Reports() {
		switch progress.kind {
		case reportKindTableStarted:
			progressTracker.MarkStarted(progress.tableName, progress.totalRows)
			m.logger.Debug(reportKindTableStarted, zap.String("table", progress.tableName))
			continue

		case reportKindTableDispatched:
			totalProcessedByTable, estRows, tableDone := progressTracker.MarkDispatched(progress.tableName, progress.tasksCount)
			m.logger.Debug(reportKindTableDispatched, zap.String("table", progress.tableName), zap.Bool("done", tableDone),
				zap.Int("tasks_count", progress.tasksCount))

			// when table has no tasks or all batches has been already completed
			if progress.tasksCount == 0 || tableDone {
				m.setTableCompleted(ctx, progress.tableName, totalProcessedByTable, estRows)
			}
		case reportKindMaxLSN:
			totalProcessedByTable, estRows, tableDone := progressTracker.AddMaxLSN(progress.tableName, progress.maxLSN)
			m.logger.Debug(reportKindMaxLSN, zap.String("table", progress.tableName), zap.Bool("done", tableDone),
				zap.String("lsn", progress.maxLSN.String()))
			if tableDone {
				m.setTableCompleted(ctx, progress.tableName, totalProcessedByTable, estRows)
			}
		case reportKindTaskDone:
			totalProcessedByTable, estRows, tableDone := progressTracker.CompleteTask(progress.tableName)
			m.logger.Debug(reportKindTaskDone, zap.String("table", progress.tableName), zap.Bool("done", tableDone))
			if tableDone {
				m.setTableCompleted(ctx, progress.tableName, totalProcessedByTable, estRows)
			}
		case reportKindLSNAck:
			tablesDone := progressTracker.Ack(progress.maxLSN)
			if len(tablesDone) > 0 {
				for _, table := range tablesDone {
					m.setTableCompleted(ctx, table, 0, 0)
				}
			}
		case reportKindRangeDone:
			totalProcessedByTable, estRows, logProgress, tableDone := progressTracker.Add(progress.tableName, progress.rowsProcessed)
			m.logger.Debug(reportKindRangeDone, zap.String("table", progress.tableName), zap.Int64("processed_rows", totalProcessedByTable),
				zap.Int64("total_rows", estRows), zap.String("lsn", progress.maxLSN.String()), zap.Bool("done", tableDone))
			err := m.stateManager.UpdateProgress(ctx, progress.tableName, totalProcessedByTable, StatusInProgress, nil)
			if err != nil {
				m.logger.Error("failed to update progress", zap.String("table", progress.tableName), zap.Error(err))
				continue
			}

			if logProgress {
				fields := []zap.Field{
					zap.String("table", progress.tableName),
					zap.Int64("batch_rows", progress.rowsProcessed),
				}

				fields = append(fields, zap.Int64("processed_rows", totalProcessedByTable))
				if estRows > 0 {
					fields = append(fields, zap.Int64("total_estimated_rows", estRows))
				}

				m.logger.Info("progress", fields...)
			}

			if tableDone {
				m.setTableCompleted(ctx, progress.tableName, totalProcessedByTable, estRows)
			}
		}
	}
}

func (m *SnapshotManager) printStatistics(ctx context.Context, progressTracker *snapshotProgressTracker) {
	t := time.NewTicker(m.snapshotStatsInterval())
	defer t.Stop()

	//var lastQueueSize uint64
	//var timeEqualStart time.Time
	for {
		select {
		case <-t.C:
			//qs := m.listener.WriteQueueSize()
			//if lastQueueSize == 0 || lastQueueSize != qs {
			//	lastQueueSize = qs
			//	timeEqualStart = time.Time{}
			//} else {
			//	m.logger.Warn("write queue size is not changing", zap.Uint64("queue_size", qs))
			//	if timeEqualStart.IsZero() {
			//		timeEqualStart = time.Now()
			//	} else if time.Since(timeEqualStart) > 5*time.Minute {
			//		DumpGoroutinesToFile("goroutines.txt")
			//		m.logger.Fatal("snapshot stuck")
			//	}
			//}

			m.logStatistics(progressTracker)
		case <-ctx.Done():
			m.logger.Debug("stopped snapshot statistics")

			return
		}
	}
}

func DumpGoroutinesToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// debug = 2 gives a more detailed, human-readable stack dump.
	return pprof.Lookup("goroutine").WriteTo(f, 2)
}

func (m *SnapshotManager) logStatistics(progressTracker *snapshotProgressTracker) {
	m.logger.Info("statistics", m.getStatsAsFields(progressTracker)...)
}

func (m *SnapshotManager) getStatsAsFields(progressTracker *snapshotProgressTracker) []zap.Field {
	stats := progressTracker.Stats()
	fields := []zap.Field{
		zap.Int("snapshot_tables_total", stats.tablesTotal),
		zap.Int("snapshot_tables_pending", stats.tablesPending),
		zap.Int("snapshot_tables_in_progress", stats.tablesInProgress),
		zap.Int("snapshot_tables_completed", stats.tablesCompleted),
		zap.Int("snapshot_tables_failed", stats.tablesFailed),
		zap.Int64("snapshot_rows_processed", stats.rowsProcessed),
		zap.Int64("snapshot_rows_total_estimated", stats.rowsTotalEstimated),
		zap.Int64("snapshot_rows_remaining_estimated", stats.rowsRemaining),
		zap.String("snapshot_acked_lsn", stats.ackedLSN.String()),
	}

	if stats.rowsTotalEstimated > 0 {
		fields = append(fields, zap.Float64("snapshot_progress_percent", stats.progressPercent))
	}

	if m.listener != nil {
		fields = append(fields, zap.Uint64("snapshot_write_queue_size", m.listener.WriteQueueSize()))
	}

	return fields
}

func (m *SnapshotManager) snapshotStatsInterval() time.Duration {
	if m.statsInterval > 0 {
		return m.statsInterval
	}

	return time.Minute
}

func (m *SnapshotManager) snapshotQueryColumns(tableName string) []string {
	if m.listener == nil {
		return nil
	}

	table := m.listener.GetTable(pgschema.ParseTableName(tableName))
	if table == nil {
		return nil
	}

	return snapshotQueryColumns(table)
}

func snapshotQueryColumns(table *pgschema.Table) []string {
	if table != nil && table.HasAllPhysicalColumns() {
		return nil
	}

	return snapshotColumnNames(table)
}

func snapshotColumnNames(table *pgschema.Table) []string {
	if table == nil || len(table.Columns) == 0 {
		return nil
	}

	columns := make([]*pgschema.Column, 0, len(table.Columns))
	for _, col := range table.Columns {
		columns = append(columns, col)
	}

	slices.SortFunc(columns, func(a, b *pgschema.Column) int {
		if a.Position < b.Position {
			return -1
		}
		if a.Position > b.Position {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})

	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, col.Name)
	}

	return names
}

func snapshotKeyMap(columns []string, txOpts appconfig.TxCommitTimeOptions) *keymap.KeyMap {
	keys := make([]string, 0, len(columns)+1)
	keys = append(keys, columns...)
	if txOpts.Save && !slices.Contains(keys, txOpts.Name) {
		keys = append(keys, txOpts.Name)
	}

	return keymap.New(keys...)
}

func (m *SnapshotManager) reportSnapshotAckProgress(ctx context.Context, reporter *snapshotProgressReporter, resp *pgwal.Response, maxAcked pglogrepl2json.CommitPoint) {
	if resp == nil {
		return
	}

	rowsByTable := make(map[string]int64)
	for _, entry := range resp.Entries {
		if entry == nil || entry.Table.FullName == "" {
			continue
		}

		rowsByTable[entry.Table.FullName]++
	}

	for tableName, rows := range rowsByTable {
		err := reporter.Report(ctx, progressReport{
			tableName:     tableName,
			rowsProcessed: rows,
			kind:          reportKindRangeDone,
		})
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			m.logger.Error("failed to report table progress",
				zap.String("table", tableName),
				zap.Error(err))
		}
	}

	if maxAcked.LSN > 0 {
		err := reporter.Report(ctx, progressReport{kind: reportKindLSNAck, maxLSN: maxAcked.LSN})

		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			m.logger.Error("failed to report snapshot acknowledgement progress", zap.String(pglogger.LSNParam, maxAcked.LSN.String()), zap.Error(err))
		}
	}
}

func (m *SnapshotManager) waitForSnapshotAcks(ctx context.Context, targetLSN pglogrepl.LSN) error {
	if targetLSN == 0 {
		return nil
	}

	const (
		pollInterval = 100 * time.Millisecond
		logInterval  = 5 * time.Second
	)

	pollTicker := time.NewTicker(pollInterval)
	logTicker := time.NewTicker(logInterval)
	defer pollTicker.Stop()
	defer logTicker.Stop()

	m.logger.Info("waiting for snapshot listener acknowledgements",
		zap.String("target_lsn", targetLSN.String()),
	)

	for {
		commitPos := m.listener.CommitPos()
		if commitPos.LSN >= targetLSN {
			m.logger.Info("all snapshot batches acknowledged",
				zap.String("target_lsn", targetLSN.String()),
				zap.String("acknowledged_lsn", commitPos.LSN.String()),
			)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pollTicker.C:
		case <-logTicker.C:
			m.logger.Info("still waiting for snapshot listener acknowledgements",
				zap.String("target_lsn", targetLSN.String()),
				zap.String("acknowledged_lsn", commitPos.LSN.String()),
			)
		}
	}
}

func (m *SnapshotManager) getTableEstimate(ctx context.Context, st *TableState) (tableEstimate, error) {
	estimate, err := m.getBaseTableEstimate(ctx, st.TableName)
	if err != nil {
		return tableEstimate{}, err
	}
	estimate.baseEstimatedRows = estimate.estimatedRows

	if st.whereClause == "" {
		return estimate, nil
	}

	filteredRows, err := m.getQueryEstimatedRows(ctx, *st.SnapshotQuery)
	if err != nil {
		return tableEstimate{}, err
	}

	estimate.estimatedRows = filteredRows

	return estimate, nil
}

func (m *SnapshotManager) planSnapshotTable(ctx context.Context, st *TableState, estimate tableEstimate, maxWorkers int, snapshotID string) (snapshotTablePlan, error) {
	if st.SnapshotQuery == nil {
		return snapshotTablePlan{}, fmt.Errorf("snapshot query for table %s is not set", st.TableName)
	}

	if st.whereClause == "" {
		return m.planCtidRanges(*st.SnapshotQuery, false, estimate, "ctid_range"), nil
	}

	if estimate.estimatedRows == 0 {
		return m.planQuerySnapshotFromBounds(ctx, st, estimate, snapshotID, "empty_query_bounds")
	}

	if m.shouldRunQuerySnapshotAsSingleTask(estimate, maxWorkers) {
		return snapshotTablePlan{
			query:    *st.SnapshotQuery,
			strategy: "single_query",
		}, nil
	}

	if m.shouldProbeSparseQueryBounds(estimate, maxWorkers) {
		plan, err := m.planQuerySnapshotFromBounds(ctx, st, estimate, snapshotID, "sparse_query_bounds")
		if err == nil {
			return plan, nil
		}

		m.logger.Warn("failed to read sparse query ctid bounds; falling back to full ctid range planning",
			zap.String("table", st.TableName),
			zap.Error(err))
	}

	return m.planCtidRanges(*st.SnapshotQuery, true, estimate, "ctid_range"), nil
}

func (m *SnapshotManager) shouldRunQuerySnapshotAsSingleTask(estimate tableEstimate, maxWorkers int) bool {
	if estimate.estimatedRows <= 0 {
		return false
	}

	threshold := int64(m.config.BatchSize * maxWorkers)
	if threshold <= 0 {
		return false
	}

	return estimate.estimatedRows <= threshold
}

func (m *SnapshotManager) shouldProbeSparseQueryBounds(estimate tableEstimate, maxWorkers int) bool {
	// Zero-estimate query snapshots are handled by the earlier empty-query probe branch.
	// This path only decides whether a non-empty estimate is sparse enough to narrow CTID ranges.
	if estimate.estimatedRows <= 0 || estimate.baseEstimatedRows <= 0 {
		return false
	}

	// A low selectivity ratio alone is not enough: 1% of a huge table can still be a large query.
	// Keep the bounds probe limited to query snapshots that are small in absolute row count too.
	//maxRowsForProbe := int64(m.config.BatchSize * maxWorkers * querySnapshotSparseMaxBatches)
	//if maxRowsForProbe <= 0 || estimate.estimatedRows > maxRowsForProbe {
	//	return false
	//}

	// Use integer math for: estimatedRows / baseEstimatedRows < 10%.
	return estimate.estimatedRows*100 < estimate.baseEstimatedRows*querySnapshotSparseSelectivityPercent
}

func (m *SnapshotManager) planQuerySnapshotFromBounds(ctx context.Context, st *TableState, estimate tableEstimate, snapshotID string, strategy string) (snapshotTablePlan, error) {
	bounds, found, err := m.getQueryCtidBounds(ctx, st, snapshotID)
	if err != nil {
		return snapshotTablePlan{}, err
	}

	// table has no data - we skip the snapshot
	if !found {
		return snapshotTablePlan{
			query:    "",
			strategy: strategy + "_none",
		}, nil
	}

	blocksPerBatch := m.blocksPerBatch(estimate, m.config.BatchSize)
	if estimate.estimatedRows <= 0 { // less than zero shouldn't be possible, but we use it as a safeguard
		// when estimated row count is zero - we use a single batch which spans the entire table
		blockWidth, err := ctidBoundsBlockWidth(bounds)
		if err != nil {
			return snapshotTablePlan{}, err
		}
		blocksPerBatch = blockWidth
	}

	ranges, err := m.splitCtidRangeBetween(bounds, blocksPerBatch)
	if err != nil {
		return snapshotTablePlan{}, err
	}

	return snapshotTablePlan{
		query:          m.queryBuilder.BuildBatchQuery(*st.SnapshotQuery, true),
		ranges:         ranges,
		usesCtidRanges: true,
		strategy:       strategy,
		blocksPerBatch: blocksPerBatch,
	}, nil
}

func (m *SnapshotManager) planCtidRanges(baseQuery string, hasWhereClause bool, estimate tableEstimate, strategy string) snapshotTablePlan {
	blocksPerBatch := m.blocksPerBatch(estimate, m.config.BatchSize)
	ranges := m.splitCtidRange(estimate.totalBlocks, blocksPerBatch)

	return snapshotTablePlan{
		query:          m.queryBuilder.BuildBatchQuery(baseQuery, hasWhereClause),
		ranges:         ranges,
		usesCtidRanges: true,
		strategy:       strategy,
		blocksPerBatch: blocksPerBatch,
	}
}

func (m *SnapshotManager) getQueryCtidBounds(ctx context.Context, st *TableState, snapshotID string) (ctidBounds, bool, error) {
	queryCtx, cancel, err := m.snapshotQueryContext(ctx)
	if err != nil {
		return ctidBounds{}, false, err
	}
	defer cancel()

	conn, err := m.connector.AcquirePrimary(queryCtx)
	if err != nil {
		return ctidBounds{}, false, fmt.Errorf("failed to acquire primary connection: %w", err)
	}
	defer conn.Close(context.Background())

	tx, err := conn.BeginTx(queryCtx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return ctidBounds{}, false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	if snapshotID != "" {
		_, err = tx.Exec(queryCtx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID))
		if err != nil {
			return ctidBounds{}, false, fmt.Errorf("failed to set transaction snapshot: %w", err)
		}
	}

	var minCtid, maxCtid pgtype.TID
	err = tx.QueryRow(queryCtx, m.buildCtidBoundsQuery(st.TableName, st.whereClause)).Scan(&minCtid, &maxCtid)
	if err != nil {
		return ctidBounds{}, false, fmt.Errorf("failed to read query ctid bounds: %w", err)
	}

	if !minCtid.Valid || !maxCtid.Valid {
		return ctidBounds{}, false, nil
	}

	return ctidBounds{min: minCtid, max: maxCtid}, true, nil
}

func (m *SnapshotManager) snapshotQueryContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if m.config == nil || m.config.QueryTimeout == "" {
		return ctx, func() {}, nil
	}

	timeout, err := time.ParseDuration(m.config.QueryTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid snapshot query timeout %q: %w", m.config.QueryTimeout, err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	return queryCtx, cancel, nil
}

func (m *SnapshotManager) buildCtidBoundsQuery(tableName string, whereClause string) string {
	var fromClause strings.Builder
	fromClause.WriteString(" FROM ")
	fromClause.WriteString(tableName)
	if whereClause != "" {
		fromClause.WriteString(" WHERE ")
		fromClause.WriteString(whereClause)
	}

	return "SELECT min(ctid), max(ctid)" + fromClause.String()
}

func (m *SnapshotManager) getBaseTableEstimate(ctx context.Context, tableName string) (tableEstimate, error) {
	return pgconnector.ExecPrimary(ctx, m.connector, func(ctx context.Context, conn *pgx.Conn) (tableEstimate, error) {
		var estimate tableEstimate
		err := conn.QueryRow(ctx,
			`SELECT CASE
				WHEN c.reltuples >= 0 AND c.relpages > 0 THEN
					(c.reltuples / c.relpages * (pg_relation_size(c.oid) / current_setting('block_size')::int))::bigint
				ELSE c.reltuples::bigint
			END AS estimated_rows,
			(pg_relation_size(c.oid) / current_setting('block_size')::int)::bigint AS total_blocks
			FROM pg_class c
			WHERE c.oid = to_regclass($1)`,
			tableName,
		).Scan(&estimate.estimatedRows, &estimate.totalBlocks)
		return estimate, err
	})
}

func (m *SnapshotManager) getQueryEstimatedRows(ctx context.Context, selectQuery string) (int64, error) {
	return pgconnector.ExecPrimary(ctx, m.connector, func(ctx context.Context, conn *pgx.Conn) (int64, error) {
		query := fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", selectQuery)
		var raw string
		err := conn.QueryRow(ctx, query).Scan(&raw)
		if err != nil {
			return 0, err
		}

		type explainPlan struct {
			Plan struct {
				PlanRows int64 `json:"Plan Rows"`
			} `json:"Plan"`
		}

		var plans []explainPlan
		if err := json.Unmarshal([]byte(raw), &plans); err != nil {
			return 0, err
		}

		if len(plans) == 0 {
			return 0, errors.New("empty explain output")
		}

		return plans[0].Plan.PlanRows, nil
	})
}

func (m *SnapshotManager) blocksPerBatch(estimate tableEstimate, batchSize int) int64 {
	// batchSize is the desired rows per snapshot chunk
	// estimate.totalBlocks is how many heap blocks the table occupies
	// estimate.estimatedRows is how many rows Postgres thinks are in the table
	// first we need to know approx. how many rows each block occupies on average:
	// estimatedRowsPerBlock = estimatedRows / totalBlocks
	// then estimated total number of blocks per batch would be:
	// blocksPerBatch = batchSize / estimatedRowsPerBlock =>
	// blocksPerBatch = batchSize / (estimatedRows / totalBlocks) =>
	// blocksPerBatch = (batchSize * totalBlocks) / estimatedRows
	if estimate.totalBlocks <= 0 {
		return 0
	}

	if batchSize <= 0 || estimate.estimatedRows <= 0 {
		return 1
	}

	blocks := ceilDiv(int64(batchSize)*estimate.totalBlocks, estimate.estimatedRows)
	if blocks <= 0 {
		return 1
	}

	return blocks
}

type ctidRange struct {
	min pgtype.TID
	max pgtype.TID
}

func (m *SnapshotManager) splitCtidRange(totalBlocks int64, blocksPerRange int64) []ctidRange {
	if totalBlocks <= 0 || blocksPerRange <= 0 {
		return nil
	}

	ranges := make([]ctidRange, 0, (totalBlocks+blocksPerRange-1)/blocksPerRange)
	for blockStart := int64(0); blockStart < totalBlocks; blockStart += blocksPerRange {
		blockEnd := min(blockStart+blocksPerRange, totalBlocks) - 1
		ranges = append(ranges, ctidRange{
			min: makeTID(blockStart, 0),
			max: makeTID(blockEnd, uint16(math.MaxUint16)),
		})
	}

	return ranges
}

func (m *SnapshotManager) splitCtidRangeBetween(bounds ctidBounds, blocksPerRange int64) ([]ctidRange, error) {
	minBlock := int64(bounds.min.BlockNumber)
	maxBlock := int64(bounds.max.BlockNumber)

	if minBlock > maxBlock {
		return nil, fmt.Errorf("invalid ctid bounds: min %v is greater than max %v", bounds.min, bounds.max)
	}
	if blocksPerRange <= 0 {
		return nil, nil
	}

	ranges := make([]ctidRange, 0, (maxBlock-minBlock+blocksPerRange)/blocksPerRange)
	for blockStart := minBlock; blockStart <= maxBlock; blockStart += blocksPerRange {
		blockEnd := min(blockStart+blocksPerRange-1, maxBlock)
		ranges = append(ranges, ctidRange{
			min: makeTID(blockStart, 0),
			max: makeTID(blockEnd, uint16(math.MaxUint16)),
		})
	}

	return ranges, nil
}

func ctidBoundsBlockWidth(bounds ctidBounds) (int64, error) {
	minBlock := int64(bounds.min.BlockNumber)
	maxBlock := int64(bounds.max.BlockNumber)
	if minBlock > maxBlock {
		return 0, fmt.Errorf("invalid ctid bounds: min %v is greater than max %v", bounds.min, bounds.max)
	}

	return maxBlock - minBlock + 1, nil
}

func makeTID(blockNumber int64, offsetNumber uint16) pgtype.TID {
	return pgtype.TID{
		BlockNumber:  uint32(blockNumber),
		OffsetNumber: offsetNumber,
		Valid:        true,
	}
}

func ceilDiv(a, b int64) int64 {
	if b <= 0 {
		return 0
	}

	return (a + b - 1) / b
}
