package pg2redis

import (
	"context"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/pg2redis/condition"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2redis/stats"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type recordingPipeliner struct {
	redis.Pipeliner
	calls [][]any
}

func (r *recordingPipeliner) Do(ctx context.Context, args ...any) *redis.Cmd {
	r.calls = append(r.calls, append([]any(nil), args...))
	return redis.NewCmd(ctx, args...)
}

func TestMultiCommandExecution(t *testing.T) {
	ctx := context.Background()
	cfg := &redisconfig.RedisTableConfig{
		ResolvedCommands: []redisconfig.RedisCommandWithCondition{
			{
				Commands: [][]string{
					{"HSET", "order:{id}", "status", "{status}"},
					{"PUBLISH", "order_updates", "{id}"},
				},
				Condition: &redisconfig.ConditionConfig{
					Column: "status",
					Op:     redisconfig.OpEqual,
					Value:  "completed",
				},
			},
		},
	}

	writer := newTestRedisDynamicWriter(cfg)

	t.Run("condition matches", func(t *testing.T) {
		pp := &recordingPipeliner{}
		entry := newTestWriteEntry("123", "completed")

		err := writer.write(ctx, pp, entry, time.Now(), 0)
		assert.NoError(t, err)
		assert.Equal(t, [][]any{
			{"MULTI"},
			{"HSET", "order:123", "status", "completed"},
			{"PUBLISH", "order_updates", "123"},
			{"EXEC"},
		}, pp.calls)
	})

	t.Run("condition does not match", func(t *testing.T) {
		pp := &recordingPipeliner{}
		entry := newTestWriteEntry("124", "pending")

		err := writer.write(ctx, pp, entry, time.Now(), 0)
		assert.NoError(t, err)
		assert.Empty(t, pp.calls)
	})
}

func TestMultipleTopLevelCommandsExecuteInOneTransaction(t *testing.T) {
	ctx := context.Background()
	cfg := &redisconfig.RedisTableConfig{
		ResolvedCommands: []redisconfig.RedisCommandWithCondition{
			{Commands: [][]string{{"HSET", "order:{id}", "status", "{status}"}}},
			{
				Commands: [][]string{{"SADD", "completed_orders", "{id}"}},
				Condition: &redisconfig.ConditionConfig{
					Column: "status",
					Op:     redisconfig.OpEqual,
					Value:  "completed",
				},
			},
			{
				Commands: [][]string{{"DEL", "pending_order:{id}"}},
				Condition: &redisconfig.ConditionConfig{
					Column: "status",
					Op:     redisconfig.OpEqual,
					Value:  "pending",
				},
			},
		},
	}

	pp := &recordingPipeliner{}
	writer := newTestRedisDynamicWriter(cfg)
	entry := newTestWriteEntry("123", "completed")

	err := writer.write(ctx, pp, entry, time.Now(), 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]any{
		{"MULTI"},
		{"HSET", "order:123", "status", "completed"},
		{"SADD", "completed_orders", "123"},
		{"EXEC"},
	}, pp.calls)
}

func TestSingleCommandExecutesWithoutTransaction(t *testing.T) {
	ctx := context.Background()
	cfg := &redisconfig.RedisTableConfig{
		ResolvedCommands: []redisconfig.RedisCommandWithCondition{
			{Commands: [][]string{{"HSET", "order:{id}", "status", "{status}"}}},
		},
	}

	pp := &recordingPipeliner{}
	writer := newTestRedisDynamicWriter(cfg)
	entry := newTestWriteEntry("123", "completed")

	err := writer.write(ctx, pp, entry, time.Now(), 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]any{
		{"HSET", "order:123", "status", "completed"},
	}, pp.calls)
}

func TestWriteReturnsErrorWhenJsonCommandCannotMarshal(t *testing.T) {
	ctx := context.Background()
	cfg := &redisconfig.RedisTableConfig{
		ResolvedCommands: []redisconfig.RedisCommandWithCondition{
			{Commands: [][]string{{"SET", "order:{id}", "{json:*}"}}},
		},
	}

	pp := &recordingPipeliner{}
	writer := newTestRedisDynamicWriter(cfg)
	entry := newTestWriteEntry("123", "completed", "bad")
	entry.Tuple.Set("bad", make(chan int))

	err := writer.write(ctx, pp, entry, time.Now(), 0)
	assert.ErrorContains(t, err, "failed to build Redis command SET")
	assert.Empty(t, pp.calls)
}

func newTestRedisDynamicWriter(cfg *redisconfig.RedisTableConfig) *redisDynamicWriter {
	logger := zap.NewNop()

	return &redisDynamicWriter{
		cfg:              cfg,
		evaluator:        condition.NewEvaluator(),
		resolvedCommands: mustCompileCommandGroups(cfg.ResolvedCommands, logger),
		insertCommands:   mustCompileCommandGroups(cfg.InsertCommands, logger),
		updateCommands:   mustCompileCommandGroups(cfg.UpdateCommands, logger),
		deleteCommands:   mustCompileCommandGroups(cfg.DeleteCommands, logger),
		redisWriter: redisWriter{
			logger: logger,
			stats:  stats.NewRedisStats("test", nil),
		},
	}
}

func newTestWriteEntry(id string, status string, extraKeys ...string) *pgwal.WriteEntry {
	keys := append([]string{"id", "status"}, extraKeys...)
	tuple := newTestOrderedMap(keys...)
	tuple.Set("id", id)
	tuple.Set("status", status)

	return &pgwal.WriteEntry{
		Kind:  pgwal.Insert,
		Table: pgschema.TableName{Schema: "public", Name: "orders"},
		PK:    id,
		Tuple: tuple,
	}
}
