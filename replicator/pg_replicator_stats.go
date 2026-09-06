package replicator

import (
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"strings"
)

const (
	txLastReported   string = "tx_last_reported"
	lsnLastReported  string = "lsn_last_reported"
	txLastCommitted  string = "tx_last_committed"
)

type Stats struct {
	TxLastReported  *pg2stats.Gauge
	LSNLastReported *pg2stats.Gauge
	TxLastCommitted *pg2stats.Gauge
}

func NewStats(prefix string) *Stats {
	low := strings.ToLower(prefix) + "_"

	return &Stats{
		TxLastReported:  pg2stats.NewGauge(low+txLastReported, "Last transaction reported as processed"),
		LSNLastReported: pg2stats.NewGauge(low+lsnLastReported, "Last LSN reported as processed"),
		TxLastCommitted: pg2stats.NewGauge(low+txLastCommitted, "Last transaction committed on the PG cluster"),
	}
}
