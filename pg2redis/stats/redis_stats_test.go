package stats

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func floatFromBits(bits int64) float64 {
	return math.Float64frombits(uint64(bits))
}

func TestNewRedisStats(t *testing.T) {
	tables := []string{"public.users", "public.posts"}
	s := NewRedisStats("test_app", tables)

	assert.NotNil(t, s)
	assert.Equal(t, "test_app_", s.prefix)
	assert.NotNil(t, s.CommandsTotal)
	assert.NotNil(t, s.FailuresTotal)
	assert.NotNil(t, s.BatchesTotal)
}

func TestRedisStats_TrackCommand(t *testing.T) {
	s := NewRedisStats("test", nil)
	s.TrackCommand("SET", "public.users")
	s.TrackCommand("SET", "public.users")
	s.TrackCommand("GET", "public.users")

	fields := s.CommandsTotal.GetStatsAsFields()
	// CounterMap.Add creates counters dynamically now.
	// We expect two counters: one for SET and one for GET (with table label appended in TrackCommand)
	assert.Len(t, fields, 2)

	// Check if values are correct (order might vary)
	foundSet := false
	foundGet := false
	for _, f := range fields {
		if f.Key == "test_redis_commands_total{command=\"SET\",table=\"public.users\"}" {
			// zap.Float64 stores as bits in Integer
			val := floatFromBits(f.Integer)
			assert.Equal(t, float64(2), val)
			foundSet = true
		}
		if f.Key == "test_redis_commands_total{command=\"GET\",table=\"public.users\"}" {
			val := floatFromBits(f.Integer)
			assert.Equal(t, float64(1), val)
			foundGet = true
		}
	}
	assert.True(t, foundSet)
	assert.True(t, foundGet)
}

func BenchmarkRedisStats_TrackCommand(b *testing.B) {
	s := NewRedisStats("benchmark", nil)
	s.TrackCommand("HSET", "public.orders")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.TrackCommand("HSET", "public.orders")
	}
}

func TestRedisStats_TrackFailure(t *testing.T) {
	s := NewRedisStats("test", nil)
	s.TrackFailure("network_error", "SET", "public.users")

	fields := s.FailuresTotal.GetStatsAsFields()
	assert.Len(t, fields, 1)
	assert.Equal(t, "test_redis_failures_total{error_type=\"network_error\",command=\"SET\",table=\"public.users\"}", fields[0].Key)
	assert.Equal(t, float64(1), floatFromBits(fields[0].Integer))
}

func TestRedisStats_GetStatsAsFields(t *testing.T) {
	s := NewRedisStats("pg2redis", []string{"table1"})

	s.BatchesFlushSizeTotal.Inc()
	s.BatchesFlushTimerTotal.Add(2)
	s.BatchesInflight.Set(5)
	s.BatchSize.Set(100)
	s.TxLastSent.Set(1000)
	s.TxLastOK.Set(999)
	s.LSNLastReported.Set(12345)
	s.LSNLastFlushed.Set(12340)
	s.LagSeconds.Set(1.5)
	s.BatchesTotal.Add("table1", 1)

	fields := s.GetStatsAsFields()

	fieldMap := make(map[string]any)
	for _, f := range fields {
		if f.Type == zapcore.Float64Type { // zap.Float64Type
			fieldMap[f.Key] = floatFromBits(f.Integer)
		} else if f.Type == zapcore.Uint64Type { // zap.Uint64Type
			fieldMap[f.Key] = uint64(f.Integer)
		} else {
			fieldMap[f.Key] = f.Interface
		}
	}

	// Dump keys for debugging
	t.Logf("fieldMap size: %d", len(fieldMap))
	for k, v := range fieldMap {
		t.Logf("Key: %s, Value: %v", k, v)
	}

	assert.Equal(t, uint64(1), fieldMap["pg2redis_redis_batches_flush_size_total"])
	assert.Equal(t, uint64(2), fieldMap["pg2redis_redis_batches_flush_timer_total"])
	assert.Equal(t, uint64(5), fieldMap["pg2redis_redis_batches_inflight"])
	assert.Equal(t, uint64(100), fieldMap["pg2redis_redis_batch_size"])
	assert.Equal(t, uint64(1000), fieldMap["pg2redis_redis_tx_last_sent"])
	assert.Equal(t, uint64(999), fieldMap["pg2redis_redis_tx_last_ok"])
	assert.Equal(t, uint64(12345), fieldMap["pg2redis_redis_lsn_last_reported"])
	assert.Equal(t, uint64(12340), fieldMap["pg2redis_redis_lsn_last_flushed"])
	assert.Equal(t, 1.5, fieldMap["pg2redis_redis_lag_seconds"])
	assert.Equal(t, float64(1), fieldMap["pg2redis_redis_batches_total{table=\"table1\"}"])
}

func TestRedisStats_Histogram(t *testing.T) {
	s := NewRedisStats("test", nil)
	// Just verify it doesn't panic
	s.BatchDurationSeconds.UpdateDuration(time.Now().Add(-100 * time.Millisecond))
	s.BatchDurationSeconds.Update(0.5)
}
