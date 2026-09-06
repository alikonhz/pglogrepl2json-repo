package snapshot

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

type snapshotProgressTracker struct {
	mu sync.RWMutex

	tables map[string]*snapshotTableProgress

	reportEvery int64 // percents
	ackedLSN    pglogrepl.LSN
}

type snapshotProgressStats struct {
	tablesTotal        int
	tablesPending      int
	tablesInProgress   int
	tablesCompleted    int
	tablesFailed       int
	rowsProcessed      int64
	rowsTotalEstimated int64
	rowsRemaining      int64
	progressPercent    float64
	ackedLSN           pglogrepl.LSN
}

func newSnapshotProgressTracker(tableStates []*TableState, reportEvery int64) *snapshotProgressTracker {
	tracker := &snapshotProgressTracker{
		tables:      make(map[string]*snapshotTableProgress, len(tableStates)),
		reportEvery: reportEvery,
	}

	for _, state := range tableStates {
		t := &snapshotTableProgress{
			processedRows: state.ProcessedRows,
			totalRows:     state.TotalRowsValue(),
			tableState:    state,
		}

		tracker.tables[state.TableName] = t
	}

	return tracker
}

func (t *snapshotProgressTracker) Ack(lsn pglogrepl.LSN) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if lsn <= t.ackedLSN {
		return nil
	}

	t.ackedLSN = lsn
	var doneTables []string
	for _, table := range t.tables {
		if table.isDone(t.ackedLSN) {
			if table.tableState.Status != StatusCompleted {
				table.tableState.Status = StatusCompleted
				doneTables = append(doneTables, table.tableState.TableName)
			}
		}
	}

	return doneTables
}

func (t *snapshotProgressTracker) MarkStarted(tableName string, totalRows int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	table, ok := t.tables[tableName]
	if !ok || table == nil || table.tableState == nil {
		return
	}

	table.totalRows = totalRows
	table.tableState.TotalRows = &table.totalRows
	table.tableState.Status = StatusInProgress
}

func (t *snapshotProgressTracker) Add(tableName string, rowsProcessed int64) (int64, int64, bool, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	table, ok := t.tables[tableName]
	if !ok {
		return 0, 0, false, false
	}

	table.processedRows += rowsProcessed
	if table.tableState != nil {
		table.tableState.ProcessedRows = table.processedRows
	}

	reportProgress := true // always report by default
	estRows := table.totalRows
	if estRows > 0 && t.reportEvery > 0 && t.reportEvery < 100 {
		table.processedSinceLastReport += rowsProcessed
		reportThreshold := estRows * t.reportEvery / 100

		if table.processedSinceLastReport >= reportThreshold {
			table.processedSinceLastReport = 0
		} else {
			reportProgress = false
		}
	}

	isDone := table.isDone(t.ackedLSN)
	if isDone {
		table.tableState.Status = StatusCompleted
	}

	return table.processedRows, estRows, reportProgress, isDone
}

func (t *snapshotProgressTracker) CompleteTask(tableName string) (int64, int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	table, ok := t.tables[tableName]
	if !ok || table == nil || table.tableState == nil {
		return 0, 0, false
	}

	table.doneTasks++

	isDone := table.isDone(t.ackedLSN)
	if isDone {
		table.tableState.Status = StatusCompleted
	}

	return table.processedRows, table.totalRows, isDone
}

func (t *snapshotProgressTracker) MarkDispatched(tableName string, tasksCount int) (int64, int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	table, ok := t.tables[tableName]
	if !ok || table == nil || table.tableState == nil {
		return 0, 0, false
	}

	table.dispatched = true
	table.tasksCount = tasksCount

	isDone := table.isDone(t.ackedLSN)
	if isDone {
		table.tableState.Status = StatusCompleted
	}

	return table.processedRows, table.totalRows, isDone
}

func (t *snapshotProgressTracker) AddMaxLSN(tableName string, maxLSN pglogrepl.LSN) (int64, int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	table, ok := t.tables[tableName]
	if !ok {
		return 0, 0, false
	}

	if table.maxLSN < maxLSN {
		table.maxLSN = maxLSN
	}

	isDone := table.isDone(t.ackedLSN)
	if isDone {
		table.tableState.Status = StatusCompleted
	}

	return table.processedRows, table.totalRows, isDone
}

func (t *snapshotProgressTracker) Stats() snapshotProgressStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	stats := snapshotProgressStats{
		tablesTotal: len(t.tables),
		ackedLSN:    t.ackedLSN,
	}

	for _, table := range t.tables {
		if table == nil || table.tableState == nil {
			continue
		}

		switch table.tableState.Status {
		case StatusInProgress:
			stats.tablesInProgress++
		case StatusCompleted:
			stats.tablesCompleted++
		case StatusFailed:
			stats.tablesFailed++
		default:
			stats.tablesPending++
		}

		stats.rowsProcessed += table.processedRows

		stats.rowsTotalEstimated += table.totalRows
		if table.totalRows > table.processedRows {
			stats.rowsRemaining += table.totalRows - table.processedRows
		}
	}

	if stats.rowsTotalEstimated > 0 {
		stats.progressPercent = float64(stats.rowsProcessed) * 100 / float64(stats.rowsTotalEstimated)
	}

	return stats
}
