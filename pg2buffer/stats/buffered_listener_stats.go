package stats

import (
	"strings"

	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

const (
	txTotal           string = "tx_total"
	txStreamedTotal   string = "tx_streamed_total"
	insertsTotal      string = "inserts_total"
	updatesTotal      string = "updates_total"
	deletesTotal      string = "deletes_total"
	snapshotReadTotal string = "snapshot_read_total"

	txLastProcessed  string = "tx_last_processed"
	lsnLastProcessed string = "lsn_last_processed"

	downstreamWriteQueueLength string = "downstream_write_queue_length"
	downstreamRetryQueueLength string = "downstream_retry_queue_length"
	downstreamTxLastSent       string = "downstream_tx_last_sent"
	downstreamLSNLastSent      string = "downstream_lsn_last_sent"
)

type BufferedListenerStats struct {
	TxCount         *pg2stats.Counter
	TxStreamedCount *pg2stats.Counter

	insertCount       *pg2stats.CounterMap
	updateCount       *pg2stats.CounterMap
	deleteCount       *pg2stats.CounterMap
	snapshotReadCount *pg2stats.CounterMap

	TxLastProcessed  *pg2stats.Gauge
	LSNLastProcessed *pg2stats.Gauge

	DownStreamWriteQueueLength *pg2stats.Gauge
	DownStreamRetryQueueLength *pg2stats.Gauge
	DownStreamLastSentTx       *pg2stats.Gauge
	DownStreamLastSentLSN      *pg2stats.Gauge
}

func NewStats(prefix string, tables []string) *BufferedListenerStats {
	low := strings.ToLower(prefix) + "_"

	return &BufferedListenerStats{
		TxCount:         pg2stats.NewCounter(low+txTotal, "Total number of processed transactions"),
		TxStreamedCount: pg2stats.NewCounter(low+txStreamedTotal, "Total number of processed streamed transactions"),

		TxLastProcessed:  pg2stats.NewGauge(low+txLastProcessed, "Last transaction read from the WAL"),
		LSNLastProcessed: pg2stats.NewGauge(low+lsnLastProcessed, "Last LSN read from the WAL"),

		insertCount:       pg2stats.NewCounterMap(low+insertsTotal, "Total number of inserts", "table", tables),
		updateCount:       pg2stats.NewCounterMap(low+updatesTotal, "Total number of updates", "table", tables),
		deleteCount:       pg2stats.NewCounterMap(low+deletesTotal, "Total number of deletes", "table", tables),
		snapshotReadCount: pg2stats.NewCounterMap(low+snapshotReadTotal, "Total number of rows read during snapshot", "table", tables),

		DownStreamWriteQueueLength: pg2stats.NewGauge(low+downstreamWriteQueueLength, "Current length of the write queue"),
		DownStreamRetryQueueLength: pg2stats.NewGauge(low+downstreamRetryQueueLength, "Current length of the retry queue"),
		DownStreamLastSentTx:       pg2stats.NewGauge(low+downstreamTxLastSent, "Last transaction sent to the downstream"),
		DownStreamLastSentLSN:      pg2stats.NewGauge(low+downstreamLSNLastSent, "Last LSN sent to the downstream"),
	}
}

func (st *BufferedListenerStats) SnapshotReadInc(tableName string, v float64) {
	if v > 0 {
		st.snapshotReadCount.Add(tableName, v)
	}
}

func (st *BufferedListenerStats) InsertInc(tableName string) {
	st.insertCount.Add(tableName, 1)
}

func (st *BufferedListenerStats) UpdateInc(tableName string) {
	st.updateCount.Add(tableName, 1)
}

func (st *BufferedListenerStats) DeleteInc(tableName string) {
	st.deleteCount.Add(tableName, 1)
}

func (st *BufferedListenerStats) WriteQueueInc() {
	st.DownStreamWriteQueueLength.Add(1)
}

func (st *BufferedListenerStats) WriteQueueDec() {
	st.DownStreamWriteQueueLength.Add(-1)
}

func (st *BufferedListenerStats) RetryQueueInc() {
	st.DownStreamRetryQueueLength.Add(1)
}

func (st *BufferedListenerStats) RetryQueueDec() {
	st.DownStreamRetryQueueLength.Add(-1)
}

func (st *BufferedListenerStats) GetStatsAsFields() []zap.Field {
	counts := st.insertCount.GetStatsAsFields()
	counts = append(counts, st.updateCount.GetStatsAsFields()...)
	counts = append(counts, st.deleteCount.GetStatsAsFields()...)

	return append([]zap.Field{
		zap.Uint64(st.TxCount.Name(), uint64(st.TxCount.Value())),
		zap.Uint64(st.TxStreamedCount.Name(), uint64(st.TxStreamedCount.Value())),

		zap.Uint64(st.TxLastProcessed.Name(), uint64(st.TxLastProcessed.Value())),
		zap.String(st.LSNLastProcessed.Name(), pglogrepl.LSN(st.LSNLastProcessed.Value()).String()),

		zap.Uint64(st.DownStreamWriteQueueLength.Name(), uint64(st.DownStreamWriteQueueLength.Value())),
		zap.Uint64(st.DownStreamRetryQueueLength.Name(), uint64(st.DownStreamRetryQueueLength.Value())),
		zap.Uint64(st.DownStreamLastSentTx.Name(), uint64(st.DownStreamLastSentTx.Value())),
	}, counts...)
}
