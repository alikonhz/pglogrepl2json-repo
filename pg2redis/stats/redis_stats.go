package stats

import (
	"fmt"
	"strings"
	"sync"

	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"go.uber.org/zap"
)

// RedisStats collects and exposes metrics for Redis operations.
type RedisStats struct {
	prefix string

	// Core metrics
	CommandsTotal          *pg2stats.CounterMap
	FailuresTotal          *pg2stats.CounterMap
	BatchesTotal           *pg2stats.CounterMap
	BatchesFlushSizeTotal  *pg2stats.Counter
	BatchesFlushTimerTotal *pg2stats.Counter
	BatchesInflight        *pg2stats.Gauge
	BatchSize              *pg2stats.Gauge
	BatchDurationSeconds   *pg2stats.Histogram

	// Replication Progress Metrics
	TxLastSent      *pg2stats.Gauge
	TxLastOK        *pg2stats.Gauge
	LSNLastReported *pg2stats.Gauge
	LSNLastFlushed  *pg2stats.Gauge
	LagSeconds      *pg2stats.Gauge

	commandLabelMu    sync.RWMutex
	commandLabelCache map[commandLabelKey]string
}

type commandLabelKey struct {
	command string
	table   string
}

// NewRedisStats initializes RedisStats with a given prefix and table names.
func NewRedisStats(prefix string, tableNames []string) *RedisStats {
	prefix = strings.ToLower(prefix)
	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}
	redisPrefix := prefix + "redis_"

	return &RedisStats{
		prefix: prefix,

		// Core metrics
		CommandsTotal:          pg2stats.NewCounterMap(redisPrefix+"commands_total", "Total number of Redis commands executed", "command", nil),
		FailuresTotal:          pg2stats.NewCounterMap(redisPrefix+"failures_total", "Total number of failed Redis commands", "error_type", nil),
		BatchesTotal:           pg2stats.NewCounterMap(redisPrefix+"batches_total", "Total number of batch flushes", "table", tableNames),
		BatchesFlushSizeTotal:  pg2stats.NewCounter(redisPrefix+"batches_flush_size_total", "Flushes triggered by size limit"),
		BatchesFlushTimerTotal: pg2stats.NewCounter(redisPrefix+"batches_flush_timer_total", "Flushes triggered by timer"),
		BatchesInflight:        pg2stats.NewGauge(redisPrefix+"batches_inflight", "Number of batches currently in-flight"),
		BatchSize:              pg2stats.NewGauge(redisPrefix+"batch_size", "Number of commands per batch"),
		BatchDurationSeconds:   pg2stats.NewHistogram(redisPrefix + "batch_duration_seconds"),

		// Replication Progress Metrics
		TxLastSent:      pg2stats.NewGauge(redisPrefix+"tx_last_sent", "Last transaction XID sent to Redis"),
		TxLastOK:        pg2stats.NewGauge(redisPrefix+"tx_last_ok", "Last transaction XID successfully processed"),
		LSNLastReported: pg2stats.NewGauge(redisPrefix+"lsn_last_reported", "Last LSN reported to Postgres"),
		LSNLastFlushed:  pg2stats.NewGauge(redisPrefix+"lsn_last_flushed", "Last LSN flushed to Redis"),
		LagSeconds:      pg2stats.NewGauge(redisPrefix+"lag_seconds", "Replication lag in seconds"),

		commandLabelCache: make(map[commandLabelKey]string),
	}
}

// GetStatsAsFields returns all collected statistics as zap fields.
func (s *RedisStats) GetStatsAsFields() []zap.Field {
	var fields []zap.Field

	fields = append(fields, s.CommandsTotal.GetStatsAsFields()...)
	fields = append(fields, s.FailuresTotal.GetStatsAsFields()...)
	fields = append(fields, s.BatchesTotal.GetStatsAsFields()...)

	fields = append(fields, zap.Uint64(s.BatchesFlushSizeTotal.Name(), uint64(s.BatchesFlushSizeTotal.Value())))
	fields = append(fields, zap.Uint64(s.BatchesFlushTimerTotal.Name(), uint64(s.BatchesFlushTimerTotal.Value())))
	fields = append(fields, zap.Uint64(s.BatchesInflight.Name(), uint64(s.BatchesInflight.Value())))
	fields = append(fields, zap.Uint64(s.BatchSize.Name(), uint64(s.BatchSize.Value())))

	fields = append(fields, zap.Uint64(s.TxLastSent.Name(), uint64(s.TxLastSent.Value())))
	fields = append(fields, zap.Uint64(s.TxLastOK.Name(), uint64(s.TxLastOK.Value())))
	fields = append(fields, zap.Uint64(s.LSNLastReported.Name(), uint64(s.LSNLastReported.Value())))
	fields = append(fields, zap.Uint64(s.LSNLastFlushed.Name(), uint64(s.LSNLastFlushed.Value())))
	fields = append(fields, zap.Float64(s.LagSeconds.Name(), s.LagSeconds.Value()))

	return fields
}

// TrackCommand increments the command counter for a given command and table.
func (s *RedisStats) TrackCommand(command, table string) {
	s.CommandsTotal.Add(s.commandLabel(command, table), 1)
}

func (s *RedisStats) commandLabel(command, table string) string {
	key := commandLabelKey{command: command, table: table}

	s.commandLabelMu.RLock()
	label, ok := s.commandLabelCache[key]
	s.commandLabelMu.RUnlock()
	if ok {
		return label
	}

	label = command + "\",table=\"" + table

	s.commandLabelMu.Lock()
	if existing, ok := s.commandLabelCache[key]; ok {
		label = existing
	} else {
		s.commandLabelCache[key] = label
	}
	s.commandLabelMu.Unlock()

	return label
}

// TrackFailure increments the failure counter for a given error type, command and table.
func (s *RedisStats) TrackFailure(errType, command, table string) {
	label := fmt.Sprintf("%s\",command=\"%s\",table=\"%s", errType, command, table)
	s.FailuresTotal.Add(label, 1)
}
