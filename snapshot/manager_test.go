package snapshot

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/replicationslot"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestBlocksPerRange_UsesEstimatedRowsPerBlock(t *testing.T) {
	mgr := &SnapshotManager{}

	t.Run("returns zero when the table has no heap blocks", func(t *testing.T) {
		blocks := mgr.blocksPerBatch(tableEstimate{estimatedRows: 35462, totalBlocks: 0}, 1000)
		assert.Equal(t, int64(0), blocks)
	})

	t.Run("falls back to one block when batch size is not positive", func(t *testing.T) {
		blocks := mgr.blocksPerBatch(tableEstimate{estimatedRows: 35462, totalBlocks: 652}, 0)
		assert.Equal(t, int64(1), blocks)
	})

	t.Run("falls back to one block when row estimate is unavailable", func(t *testing.T) {
		blocks := mgr.blocksPerBatch(tableEstimate{estimatedRows: 0, totalBlocks: 652}, 1000)
		assert.Equal(t, int64(1), blocks)
	})

	t.Run("converts target rows to blocks using estimated density", func(t *testing.T) {
		blocks := mgr.blocksPerBatch(tableEstimate{estimatedRows: 35462, totalBlocks: 652}, 1000)
		assert.Equal(t, int64(19), blocks)
	})
}

func TestSplitCtidRange_UsesBlockChunks(t *testing.T) {
	mgr := &SnapshotManager{}

	t.Run("skips empty tables", func(t *testing.T) {
		assert.Nil(t, mgr.splitCtidRange(0, 4))
	})

	t.Run("skips invalid block widths", func(t *testing.T) {
		assert.Nil(t, mgr.splitCtidRange(10, 0))
	})

	t.Run("single chunk covers all blocks when chunk width exceeds table size", func(t *testing.T) {
		ranges := mgr.splitCtidRange(10, 20)
		assert.Equal(t, []ctidRange{
			{min: makeTID(0, 0), max: makeTID(9, 65535)},
		}, ranges)
	})

	t.Run("splits blocks into fixed-width block chunks", func(t *testing.T) {
		ranges := mgr.splitCtidRange(10, 3)
		assert.Equal(t, []ctidRange{
			{min: makeTID(0, 0), max: makeTID(2, 65535)},
			{min: makeTID(3, 0), max: makeTID(5, 65535)},
			{min: makeTID(6, 0), max: makeTID(8, 65535)},
			{min: makeTID(9, 0), max: makeTID(9, 65535)},
		}, ranges)
	})

	t.Run("creates trailing partial chunk", func(t *testing.T) {
		ranges := mgr.splitCtidRange(5, 2)
		assert.Equal(t, []ctidRange{
			{min: makeTID(0, 0), max: makeTID(1, 65535)},
			{min: makeTID(2, 0), max: makeTID(3, 65535)},
			{min: makeTID(4, 0), max: makeTID(4, 65535)},
		}, ranges)
	})
}

func TestSplitCtidRangeBetween_UsesBoundedBlockChunks(t *testing.T) {
	mgr := &SnapshotManager{}

	ranges, err := mgr.splitCtidRangeBetween(ctidBounds{min: makeTID(3, 8), max: makeTID(7, 2)}, 2)

	require.NoError(t, err)
	assert.Equal(t, []ctidRange{
		{min: makeTID(3, 0), max: makeTID(4, 65535)},
		{min: makeTID(5, 0), max: makeTID(6, 65535)},
		{min: makeTID(7, 0), max: makeTID(7, 65535)},
	}, ranges)
}

func TestPlanSnapshotTable_UsesSingleQueryForSmallQuerySnapshot(t *testing.T) {
	query := "SELECT * FROM public.orders WHERE  ( status = 'completed' ) "
	state := &TableState{
		TableName:     "public.orders",
		SnapshotQuery: &query,
		whereClause:   " ( status = 'completed' ) ",
	}
	mgr := &SnapshotManager{
		config: &appconfig.SnapshotConfig{
			BatchSize: 100,
		},
		queryBuilder: NewQueryBuilder(),
		logger:       zap.NewNop(),
	}

	plan, err := mgr.planSnapshotTable(context.Background(), state, tableEstimate{
		estimatedRows:     150,
		baseEstimatedRows: 10000,
		totalBlocks:       1000,
	}, 2, "")

	require.NoError(t, err)
	assert.Equal(t, "single_query", plan.strategy)
	assert.Equal(t, query, plan.query)
	assert.False(t, plan.usesCtidRanges)
	assert.Empty(t, plan.ranges)
}

func TestPlanCtidRanges_UsesParameterizedQueryWithTypedRanges(t *testing.T) {
	mgr := &SnapshotManager{
		config: &appconfig.SnapshotConfig{
			BatchSize: 100,
		},
		queryBuilder: NewQueryBuilder(),
	}

	plan := mgr.planCtidRanges("SELECT * FROM public.users", false, tableEstimate{
		estimatedRows: 100,
		totalBlocks:   10,
	}, "ctid_range")

	assert.Equal(t, "ctid_range", plan.strategy)
	assert.True(t, plan.usesCtidRanges)
	assert.Equal(t, "SELECT * FROM public.users WHERE ctid >= $1 AND ctid <= $2 ORDER BY ctid", plan.query)
	assert.Equal(t, []ctidRange{
		{min: makeTID(0, 0), max: makeTID(9, 65535)},
	}, plan.ranges)
}

func TestPlanCtidRanges_SkipsEmptyTable(t *testing.T) {
	mgr := &SnapshotManager{
		config: &appconfig.SnapshotConfig{
			BatchSize: 100,
		},
		queryBuilder: NewQueryBuilder(),
	}

	plan := mgr.planCtidRanges("SELECT * FROM public.empty", false, tableEstimate{
		estimatedRows: 0,
		totalBlocks:   0,
	}, "ctid_range")

	assert.Equal(t, "ctid_range", plan.strategy)
	assert.True(t, plan.usesCtidRanges)
	assert.Empty(t, plan.ranges)
}

func TestShouldProbeSparseQueryBounds(t *testing.T) {
	mgr := &SnapshotManager{
		config: &appconfig.SnapshotConfig{
			BatchSize: 10,
		},
	}

	assert.True(t, mgr.shouldProbeSparseQueryBounds(tableEstimate{
		estimatedRows:     50,
		baseEstimatedRows: 10000,
	}, 1))
	assert.True(t, mgr.shouldProbeSparseQueryBounds(tableEstimate{
		estimatedRows:     150,
		baseEstimatedRows: 10000,
	}, 1), "estimate is below 10% even when it is above the disabled sparse probe cap")
	assert.False(t, mgr.shouldProbeSparseQueryBounds(tableEstimate{
		estimatedRows:     150,
		baseEstimatedRows: 1000,
	}, 2), "estimate is not selective enough")
}

func TestBuildCtidBoundsQuery(t *testing.T) {
	mgr := &SnapshotManager{}

	query := mgr.buildCtidBoundsQuery("public.orders", " ( status = 'completed' ) ")

	assert.Equal(t,
		"SELECT min(ctid), max(ctid) FROM public.orders WHERE  ( status = 'completed' ) ",
		query)
}

func TestSnapshotQueryColumns(t *testing.T) {
	table := pgschema.NewTable("public", "orders")
	table.AddColumn(&pgschema.Column{Name: "status", Position: 2})
	table.AddColumn(&pgschema.Column{Name: "id", Position: 1})

	assert.Equal(t, []string{"id", "status"}, snapshotQueryColumns(table))

	table.PhysicalColumnsCount = 2
	assert.Nil(t, snapshotQueryColumns(table))
}

func TestSnapshotProgressTracker_Add(t *testing.T) {
	totalRows := int64(500)
	usersState := &TableState{
		TableName: "public.users",
		TotalRows: &totalRows,
	}
	ordersState := &TableState{
		TableName: "public.orders",
	}

	tracker := newSnapshotProgressTracker([]*TableState{usersState, ordersState}, 10)

	processed, estimated, reportProgress, _ := tracker.Add("public.users", 100)
	assert.Equal(t, int64(100), processed)
	assert.Equal(t, totalRows, estimated)
	assert.True(t, reportProgress) // we processed more than 10% so we should report progress
	assert.Equal(t, int64(100), usersState.ProcessedRows)

	processed, estimated, reportProgress, _ = tracker.Add("public.users", 100)
	assert.Equal(t, int64(200), processed)
	assert.Equal(t, totalRows, estimated)
	assert.True(t, reportProgress) // we processed more than 10% so we should report progress
	assert.Equal(t, int64(200), usersState.ProcessedRows)

	processed, estimated, reportProgress, _ = tracker.Add("public.orders", 7)
	assert.Equal(t, int64(7), processed)
	assert.Equal(t, int64(0), estimated)
	assert.True(t, reportProgress) // estimated count is zero - so we always report progress
	assert.Equal(t, int64(7), ordersState.ProcessedRows)

	processed, estimated, reportProgress, _ = tracker.Add("public.users", 100)
	assert.Equal(t, int64(300), processed)
	assert.Equal(t, totalRows, estimated)
	assert.Equal(t, int64(300), usersState.ProcessedRows)

	processed, estimated, reportProgress, _ = tracker.Add("public.users", 10)
	assert.Equal(t, int64(310), processed)
	assert.Equal(t, totalRows, estimated)
	assert.False(t, reportProgress) // we processed less than 10% so we should not report progress
	assert.Equal(t, int64(310), usersState.ProcessedRows)
}

func TestSnapshotProgressTracker_TableCompletedBeforeBatches(t *testing.T) {
	totalRows := int64(500)
	usersState := &TableState{
		TableName: "public.users",
		TotalRows: &totalRows,
	}

	tracker := newSnapshotProgressTracker([]*TableState{usersState}, 10)

	// before sending the task to workers the manager reports batch start
	_, _, tableDone := tracker.MarkDispatched("public.users", 1)
	assert.False(t, tableDone)

	tracker.AddMaxLSN("public.users", 1) // range #1 batch #1
	tracker.AddMaxLSN("public.users", 2) // range #1 batch #2

	assert.Equal(t, 1, tracker.tables["public.users"].tasksCount)
	assert.Equal(t, 0, tracker.tables["public.users"].doneTasks)
	assert.Equal(t, pglogrepl.LSN(2), tracker.tables["public.users"].maxLSN)

	// range #1 notification done
	processed, _, tableDone := tracker.CompleteTask("public.users")
	assert.Equal(t, int64(0), processed)
	assert.False(t, tableDone) // not all ranges are done yet so table is not completed yet
	assert.Equal(t, 1, tracker.tables["public.users"].tasksCount)
	assert.Equal(t, 1, tracker.tables["public.users"].doneTasks)

	// range #1 batch #1
	processed, _, _, tableDone = tracker.Add("public.users", 100)
	assert.Equal(t, int64(100), processed)
	assert.False(t, tableDone)
	assert.Equal(t, int64(100), tracker.tables["public.users"].processedRows)

	tracker.Ack(pglogrepl.LSN(2))
	processed, _, _, tableDone = tracker.Add("public.users", 300) // range #1 batch #2
	assert.Equal(t, int64(400), processed)
	assert.True(t, tableDone)
	assert.Equal(t, int64(400), tracker.tables["public.users"].processedRows)
}

func TestSnapshotProgressTrackerStats(t *testing.T) {
	usersRows := int64(100)
	ordersRows := int64(50)
	tracker := newSnapshotProgressTracker([]*TableState{
		{
			TableName:     "public.users",
			TotalRows:     &usersRows,
			ProcessedRows: 25,
			Status:        StatusInProgress,
		},
		{
			TableName:     "public.orders",
			TotalRows:     &ordersRows,
			ProcessedRows: 50,
			Status:        StatusCompleted,
		},
		{
			TableName: "public.events",
			Status:    StatusPending,
		},
	}, 10)

	tracker.Ack(pglogrepl.LSN(12))

	stats := tracker.Stats()
	assert.Equal(t, 3, stats.tablesTotal)
	assert.Equal(t, 1, stats.tablesPending)
	assert.Equal(t, 1, stats.tablesInProgress)
	assert.Equal(t, 1, stats.tablesCompleted)
	assert.Equal(t, int64(75), stats.rowsProcessed)
	assert.Equal(t, int64(150), stats.rowsTotalEstimated)
	assert.Equal(t, int64(75), stats.rowsRemaining)
	assert.Equal(t, float64(50), stats.progressPercent)
	assert.Equal(t, pglogrepl.LSN(12), stats.ackedLSN)
}

func TestSnapshotManagerPrintStatistics(t *testing.T) {
	core, observedLogs := observer.New(zap.InfoLevel)
	mgr := &SnapshotManager{
		logger:        zap.New(core),
		statsInterval: 5 * time.Millisecond,
	}
	tracker := newSnapshotProgressTracker([]*TableState{
		{
			TableName:     "public.users",
			ProcessedRows: 10,
			Status:        StatusInProgress,
		},
	}, 10)

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		mgr.printStatistics(ctx, tracker)
	}()

	require.Eventually(t, func() bool {
		return observedLogs.FilterMessage("statistics").Len() > 0
	}, time.Second, time.Millisecond)

	cancel()
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("snapshot statistics printer did not stop")
	}
}

func TestSnapshotProgressReporterDoesNotBlockWhenReportsAreNotDrained(t *testing.T) {
	reporter := newSnapshotProgressReporterAndDispatch(0, zap.NewNop())

	reportedCh := make(chan error, 1)
	go func() {
		for i := 0; i < 256; i++ {
			if err := reporter.Report(context.Background(), progressReport{
				tableName:     "public.users",
				rowsProcessed: 1,
			}); err != nil {
				reportedCh <- err
				return
			}
		}

		reportedCh <- nil
	}()

	select {
	case err := <-reportedCh:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("reporting progress blocked while reports were not being drained")
	}

	reporter.Close()

	var reportCount int
	for range reporter.Reports() {
		reportCount++
	}

	assert.Equal(t, 256, reportCount)
}

func TestPrepareTableState(t *testing.T) {
	mgr := &SnapshotManager{
		appName:      "test-app",
		queryBuilder: NewQueryBuilder(),
	}

	t.Run("preserves completed table state so it can be skipped", func(t *testing.T) {
		query := "SELECT * FROM public.users"
		totalRows := int64(10)
		existing := &TableState{
			AppName:       "test-app",
			TableName:     "public.users",
			SnapshotLSN:   "0/1",
			SnapshotType:  string(appconfig.SnapshotTableFull),
			SnapshotQuery: &query,
			TotalRows:     &totalRows,
			ProcessedRows: 10,
			Status:        StatusCompleted,
		}

		state, shouldUpsert := mgr.prepareTableState("public.users", &appconfig.SnapshotTableConfig{
			Type: appconfig.SnapshotTableFull,
		}, existing, "0/2")

		require.Same(t, existing, state)
		assert.False(t, shouldUpsert)
		assert.Equal(t, "0/1", state.SnapshotLSN)
		assert.Equal(t, int64(10), state.ProcessedRows)
		assert.Equal(t, StatusCompleted, state.Status)
		require.NotNil(t, state.TotalRows)
		assert.Equal(t, totalRows, *state.TotalRows)
	})

	t.Run("resets non-completed table state for a fresh attempt", func(t *testing.T) {
		totalRows := int64(10)
		errorMessage := "previous failure"
		existing := &TableState{
			AppName:       "test-app",
			TableName:     "public.users",
			SnapshotLSN:   "0/1",
			TotalRows:     &totalRows,
			ProcessedRows: 7,
			Status:        StatusFailed,
			ErrorMessage:  &errorMessage,
		}

		state, shouldUpsert := mgr.prepareTableState("public.users", &appconfig.SnapshotTableConfig{
			Type: appconfig.SnapshotTableFull,
		}, existing, "0/2")

		require.Same(t, existing, state)
		assert.True(t, shouldUpsert)
		assert.Equal(t, "0/2", state.SnapshotLSN)
		assert.Equal(t, string(appconfig.SnapshotTableFull), state.SnapshotType)
		require.NotNil(t, state.SnapshotQuery)
		assert.Equal(t, "SELECT * FROM public.users", *state.SnapshotQuery)
		assert.Nil(t, state.TotalRows)
		assert.Equal(t, int64(0), state.ProcessedRows)
		assert.Equal(t, StatusPending, state.Status)
		assert.Nil(t, state.ErrorMessage)
	})

	t.Run("builds query snapshot state from where clause", func(t *testing.T) {
		state, shouldUpsert := mgr.prepareTableState("public.orders", &appconfig.SnapshotTableConfig{
			Type:  appconfig.SnapshotTableQuery,
			Query: " status = 'completed' ",
		}, nil, "0/3")

		assert.True(t, shouldUpsert)
		assert.Equal(t, "public.orders", state.TableName)
		assert.Equal(t, "test-app", state.AppName)
		assert.Equal(t, "0/3", state.SnapshotLSN)
		assert.Equal(t, string(appconfig.SnapshotTableQuery), state.SnapshotType)
		require.NotNil(t, state.SnapshotQuery)
		assert.Equal(t, "SELECT * FROM public.orders WHERE  ( status = 'completed' ) ", *state.SnapshotQuery)
		assert.Equal(t, " ( status = 'completed' ) ", state.whereClause)
		assert.Equal(t, StatusPending, state.Status)
	})
}

func TestPrepareSnapshot_UsesReplicationSlotSnapshot(t *testing.T) {
	lsn, err := pglogrepl.ParseLSN("0/16B6C50")
	require.NoError(t, err)

	mgr := (&SnapshotManager{
		logger: zap.NewNop(),
	}).WithSlotSnapshot(replicationslot.NewSnapshot(lsn, "00000003-0000001B-1", nil))

	snapshotLSN, snapshotID, cleanup, err := mgr.prepareSnapshot(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "0/16B6C50", snapshotLSN)
	assert.Equal(t, "00000003-0000001B-1", snapshotID)
	require.NotNil(t, cleanup)

	cleanup(false)
}

func TestRunSnapshot_DoesNotDeadlockWhenManyTablesAreMissing(t *testing.T) {
	const tableCount = 8

	tableStates := make([]*TableState, 0, tableCount)
	for i := 0; i < tableCount; i++ {
		tableStates = append(tableStates, &TableState{
			TableName: fmt.Sprintf("public.missing_%d", i),
			Status:    StatusPending,
		})
	}

	stateStore := &fakeSnapshotStateStore{}
	mgr := &SnapshotManager{
		config: &appconfig.SnapshotConfig{
			AbortOnError:    true,
			BatchSize:       1000,
			ParallelWorkers: 1,
		},
		stateManager: stateStore,
		listener:     &ackingSnapshotListener{},
		queryBuilder: NewQueryBuilder(),
		logger:       zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.runSnapshot(ctx, tableStates, "")
	}()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot finished with errors")
		assert.Len(t, stateStore.failedTables, tableCount)
	case <-ctx.Done():
		t.Fatal("runSnapshot deadlocked while reporting missing tables")
	}
}

type fakeSnapshotStateStore struct {
	failedTables []string
}

func (s *fakeSnapshotStateStore) CreateSnapshotStateTableIfNotExists(ctx context.Context) error {
	return nil
}

func (s *fakeSnapshotStateStore) LoadTablesState(ctx context.Context) (map[string]*TableState, error) {
	return nil, nil
}

func (s *fakeSnapshotStateStore) UpsertTableState(ctx context.Context, state *TableState) error {
	return nil
}

func (s *fakeSnapshotStateStore) UpdateProgress(ctx context.Context, tableName string, processedRows int64, status SnapshotStatus, errorMessage *string) error {
	if status == StatusFailed {
		s.failedTables = append(s.failedTables, tableName)
	}
	return nil
}

func (s *fakeSnapshotStateStore) SetInProgress(ctx context.Context, tableName string, totalRows int64) error {
	return nil
}

type ackingSnapshotListener struct {
	commitPos  pglogrepl2json.CommitPoint
	ackHandler func(*pgwal.Response, pglogrepl2json.CommitPoint)
}

func (l *ackingSnapshotListener) WriteQueueSize() uint64 {
	return 0
}

func (l *ackingSnapshotListener) TxOptions() appconfig.TxCommitTimeOptions {
	return appconfig.NewTxCommitTimeOptions("")
}

func (l *ackingSnapshotListener) OnSnapshotBatch(ctx context.Context, tableName pgschema.TableName, batch *pgwal.WriteRequest) error {
	return nil
}

func (l *ackingSnapshotListener) SetSnapshotAckHandler(handler func(*pgwal.Response, pglogrepl2json.CommitPoint)) {
	l.ackHandler = handler
}

func (l *ackingSnapshotListener) GetTable(tableName pgschema.TableName) *pgschema.Table {
	return nil
}

func (l *ackingSnapshotListener) CommitPos() pglogrepl2json.CommitPoint {
	return l.commitPos
}

func (l *ackingSnapshotListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return nil
}

func (l *ackingSnapshotListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	return "", nil
}

func (l *ackingSnapshotListener) GracefulShutdown(ctx context.Context) error {
	return nil
}

func TestWaitForSnapshotAcks(t *testing.T) {
	t.Run("returns immediately when there are no submitted batches", func(t *testing.T) {
		mgr := &SnapshotManager{
			listener: &ackingSnapshotListener{},
			logger:   zap.NewNop(),
		}

		require.NoError(t, mgr.waitForSnapshotAcks(context.Background(), 0))
	})

	t.Run("waits until the listener acknowledges the max submitted fake lsn", func(t *testing.T) {
		listener := &ackingSnapshotListener{}
		mgr := &SnapshotManager{
			listener: listener,
			logger:   zap.NewNop(),
		}

		go func() {
			time.Sleep(50 * time.Millisecond)
			listener.commitPos = pglogrepl2json.CommitPoint{LSN: pglogrepl.LSN(7)}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		require.NoError(t, mgr.waitForSnapshotAcks(ctx, pglogrepl.LSN(7)))
	})

	t.Run("returns context error when acknowledgements never catch up", func(t *testing.T) {
		mgr := &SnapshotManager{
			listener: &ackingSnapshotListener{},
			logger:   zap.NewNop(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := mgr.waitForSnapshotAcks(ctx, pglogrepl.LSN(2))
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
