package snapshot

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/entrypool"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

// Worker pool for parallel snapshots
type Worker struct {
	id         int
	connector  *pgconnector.PGConnector
	listener   pglogrepl2json.SnapshotListener
	logger     *zap.Logger
	batchSize  int
	snapshotID string
	lsnTracker *snapshotLSNTracker
	txOpts     appconfig.TxCommitTimeOptions

	pool *entrypool.EntryTuplePool
}

type snapshotLSNTracker struct {
	nextLSN      atomic.Uint64
	maxSubmitted atomic.Uint64
}

func (t *snapshotLSNTracker) NextLSN() pglogrepl.LSN {
	return pglogrepl.LSN(t.nextLSN.Add(1))
}

func (t *snapshotLSNTracker) MaxLSN() pglogrepl.LSN {
	return pglogrepl.LSN(t.nextLSN.Load())
}

func (t *snapshotLSNTracker) MarkSubmitted(lsn pglogrepl.LSN) {
	submitted := uint64(lsn)

	for {
		current := t.maxSubmitted.Load()
		if submitted <= current {
			return
		}

		if t.maxSubmitted.CompareAndSwap(current, submitted) {
			return
		}
	}
}

func (t *snapshotLSNTracker) MaxSubmitted() pglogrepl.LSN {
	return pglogrepl.LSN(t.maxSubmitted.Load())
}

func NewWorker(id int, connector *pgconnector.PGConnector,
	listener pglogrepl2json.SnapshotListener, batchSize int, snapshotID string, lsnTracker *snapshotLSNTracker,
	txOpts appconfig.TxCommitTimeOptions, pool *entrypool.EntryTuplePool,
	logger *zap.Logger) *Worker {
	return &Worker{
		id:         id,
		connector:  connector,
		listener:   listener,
		logger:     logger.Named(fmt.Sprintf("worker-%d", id)),
		batchSize:  batchSize,
		snapshotID: snapshotID,
		lsnTracker: lsnTracker,
		txOpts:     txOpts,

		pool: pool,
	}
}

func (w *Worker) Process(ctx context.Context, id int, taskCh <-chan snapshotTask, errChan chan<- tableError, progressReporter *snapshotProgressReporter) {
	var conn *pgx.Conn
	defer func() {
		if conn == nil {
			return
		}

		if err := conn.Close(context.Background()); err != nil {
			w.logger.Warn("failed to close snapshot worker connection", zap.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-taskCh:
			if !ok {
				return
			}

			if conn == nil {
				var err error
				conn, err = w.connector.AcquirePrimary(ctx)
				if err != nil {
					w.logger.Error("failed to acquire primary connection", zap.Int("worker_id", id), zap.String("table", task.state.TableName), zap.Error(err))
					errChan <- tableError{tableName: task.state.TableName, err: fmt.Errorf("snapshot: process table range failed: failed to acquire primary connection: %w", err)}
					return
				}
			}

			err := w.processTableRange(ctx, conn, tableRangeTask{
				table:  task.table,
				keyMap: task.keyMap,
				query:  task.query,
				args:   task.args,
			}, progressReporter)

			if dbg := w.logger.Check(zap.DebugLevel, "finished table range"); dbg != nil {
				dbg.Write(zap.String("table", task.state.TableName))
			}

			if err != nil {
				w.logger.Error("failed", zap.Int("worker_id", id), zap.String("table", task.state.TableName), zap.Error(err))
				// we abort on first error
				errChan <- tableError{tableName: task.state.TableName, err: fmt.Errorf("snapshot: process table range failed: %w", err)}
				return
			}

			err = progressReporter.Report(ctx, progressReport{tableName: task.table.FullName(), kind: reportKindTaskDone})
			if err != nil {
				w.logger.Error("failed to report range done", zap.String("table", task.table.FullName()), zap.Error(err))
				errChan <- tableError{tableName: task.table.FullName(), err: fmt.Errorf("snapshot: process table range failed: failed to report progress: %w", err)}
				return
			}
		}
	}
}

func (w *Worker) ProcessTableRange(ctx context.Context, table *pgschema.Table, reporter *snapshotProgressReporter, query string, args ...any) error {
	conn, err := w.connector.AcquirePrimary(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire primary connection: %w", err)
	}
	defer conn.Close(context.Background())

	return w.processTableRange(ctx, conn, tableRangeTask{
		table: table,
		query: query,
		args:  args,
	}, reporter)
}

type tableRangeTask struct {
	table  *pgschema.Table
	keyMap *keymap.KeyMap
	query  string
	args   []any
}

func (w *Worker) processTableRange(ctx context.Context, conn *pgx.Conn, rangeTask tableRangeTask, reporter *snapshotProgressReporter) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	if w.snapshotID != "" {
		_, err = tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", w.snapshotID))
		if err != nil {
			return fmt.Errorf("failed to set transaction snapshot: %w", err)
		}
	}

	rows, err := tx.Query(ctx, rangeTask.query, rangeTask.args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	rowKeyMap := rangeTask.keyMap
	if rowKeyMap == nil {
		rowKeyMap = snapshotKeyMapFromFields(fieldDescriptions, w.txOpts)
	}

	for _, fd := range fieldDescriptions {
		if rowKeyMap.GetIndex(fd.Name) < 0 {
			return fmt.Errorf("snapshot keymap for table %s is missing query column %q", rangeTask.table.FullName(), fd.Name)
		}
	}

	var (
		batchCount uint32
		entries    []*pgwal.WriteEntry
	)

	sendBatch := func(entries []*pgwal.WriteEntry, batchCount uint32) error {
		lsn := w.lsnTracker.NextLSN()
		err = w.listener.OnSnapshotBatch(ctx, rangeTask.table.Name, pgwal.NewSnapshotRequest(lsn, entries))
		if err != nil {
			return fmt.Errorf("listener failed to process row: %w", err)
		}

		err = reporter.Report(ctx, progressReport{tableName: rangeTask.table.FullName(), kind: reportKindMaxLSN, maxLSN: lsn})
		if err != nil {
			return fmt.Errorf("failed to report progress %s: %w", reportKindMaxLSN, err)
		}

		w.lsnTracker.MarkSubmitted(lsn)

		return nil
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// in the snapshot we always have version = "1"
		// if we ever need to support pooling during WAL replay, we would use an actual version
		// we version to distinguish between different versions of the keyMaps
		rowMap := w.pool.Get(rangeTask.table.FullName(), "1", rowKeyMap)
		for i, fd := range fieldDescriptions {
			rowMap.Set(fd.Name, values[i])
		}

		entries = append(entries, &pgwal.WriteEntry{
			Tuple:     rowMap,
			PrevTuple: nil,
			PK:        pgschema.CreatePK(rangeTask.table, rowMap),
			Kind:      pgwal.SnapshotRead,
			Table:     rangeTask.table.Name,
			Offset:    batchCount,
		})

		batchCount++

		if batchCount >= uint32(w.batchSize) {
			err = sendBatch(entries, batchCount)
			if err != nil {
				return err
			}

			entries = nil
			batchCount = 0
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read rows: %w", err)
	}

	if batchCount > 0 {
		err = sendBatch(entries, batchCount)
		if err != nil {
			return err
		}
	}

	return nil
}

func snapshotKeyMapFromFields(fields []pgconn.FieldDescription, txOpts appconfig.TxCommitTimeOptions) *keymap.KeyMap {
	columns := make([]string, 0, len(fields))
	for _, fd := range fields {
		columns = append(columns, fd.Name)
	}

	return snapshotKeyMap(columns, txOpts)
}
