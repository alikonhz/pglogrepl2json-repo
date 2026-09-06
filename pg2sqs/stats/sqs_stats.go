package stats

import (
	"strings"

	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"go.uber.org/zap"
)

func NewSQSStats(prefix string, queueNames []string) *SQSStats {
	prefix = strings.ToLower(prefix) + "_"

	return &SQSStats{
		RequestsTotal:         pg2stats.NewCounterMap(prefix+"sqs_request_total", "Total number of requests", "queue", queueNames),
		FailuresTotal:         pg2stats.NewCounterMap(prefix+"sqs_failure_total", "Total number of failures", "queue", queueNames),
		RequestsInflightTotal: pg2stats.NewGauge(prefix+"sqs_request_inflight_total", "Total number of inflight requests"),
		TxLastSent:            pg2stats.NewGauge(prefix+"sqs_tx_last_sent", "Last transaction sent to SQS"),
		TxLastOK:              pg2stats.NewGauge(prefix+"sqs_tx_last_ok", "Last transaction successfully processed by SQS"),
	}
}

type SQSStats struct {
	RequestsTotal         *pg2stats.CounterMap
	FailuresTotal         *pg2stats.CounterMap
	RequestsInflightTotal *pg2stats.Gauge
	TxLastSent            *pg2stats.Gauge
	TxLastOK              *pg2stats.Gauge
}

func (s *SQSStats) GetStatsAsFields() []zap.Field {
	allFields := s.RequestsTotal.GetStatsAsFields()
	allFields = append(allFields, s.FailuresTotal.GetStatsAsFields()...)
	allFields = append(allFields, zap.Uint64(s.RequestsInflightTotal.Name(), uint64(s.RequestsInflightTotal.Value())))
	allFields = append(allFields, zap.Uint64(s.TxLastSent.Name(), uint64(s.TxLastSent.Value())))
	allFields = append(allFields, zap.Uint64(s.TxLastOK.Name(), uint64(s.TxLastOK.Value())))

	return allFields
}
