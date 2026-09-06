package pg2redis

import (
	"context"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/parallelio"
	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2redis/stats"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type benchmarkPipeliner struct {
	redis.Pipeliner
	commands int
}

func (p *benchmarkPipeliner) Do(ctx context.Context, args ...any) *redis.Cmd {
	p.commands++
	return redis.NewCmd(ctx, args...)
}

func (p *benchmarkPipeliner) Exec(context.Context) ([]redis.Cmder, error) {
	return nil, nil
}

type benchmarkPipelineClient struct {
	pipeline *benchmarkPipeliner
}

func (c benchmarkPipelineClient) Pipeline() redis.Pipeliner {
	c.pipeline.commands = 0
	return c.pipeline
}

type discardResponseTracker struct{}

func (discardResponseTracker) OnSuccess(*pgwal.Response) pglogrepl2json.CommitPoint {
	return pglogrepl2json.CommitPoint{}
}

func (discardResponseTracker) OnError(*pgwal.ErrResponse) {}

func BenchmarkRedisDynamicWriterBuildCommand(b *testing.B) {
	writer := benchmarkRedisDynamicWriter([]redisconfig.RedisCommandConfig{
		{"HSET", "{%schema%}.{%table%}:{%pk%}", "{pairs:*}"},
	}, appconfig.TxCommitTimeOptions{})
	entry := benchmarkWriteEntry()
	cmd := writer.resolvedCommands[0].commands[0]
	commitTime := time.Unix(1_700_000_000, 123)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := writer.buildCommand(cmd, entry, commitTime, 123); err != nil || !ok {
			b.Fatalf("buildCommand failed: ok=%v err=%v", ok, err)
		}
	}
}

func BenchmarkRedisDynamicWriterWrite(b *testing.B) {
	tests := []struct {
		name     string
		commands []redisconfig.RedisCommandConfig
	}{
		{
			name:     "hset_pairs_star",
			commands: []redisconfig.RedisCommandConfig{{"HSET", "{%schema%}.{%table%}:{%pk%}", "{pairs:*}"}},
		},
		{
			name:     "set_json_star",
			commands: []redisconfig.RedisCommandConfig{{"SET", "{%schema%}.{%table%}:{%pk%}", "{json:*}"}},
		},
		{
			name: "transaction_three_commands",
			commands: []redisconfig.RedisCommandConfig{
				{"HSET", "order:{id}", "status", "{status}"},
				{"SADD", "orders:{status}", "{id}"},
				{"PUBLISH", "order_updates", "{json:*}"},
			},
		},
	}

	ctx := context.Background()
	entry := benchmarkWriteEntry()
	commitTime := time.Unix(1_700_000_000, 123)

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			writer := benchmarkRedisDynamicWriter(tt.commands, appconfig.TxCommitTimeOptions{})
			pipeline := &benchmarkPipeliner{}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pipeline.commands = 0
				if err := writer.write(ctx, pipeline, entry, commitTime, 123); err != nil {
					b.Fatalf("write failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkRedisBatchBufferFlushBatch(b *testing.B) {
	ctx := context.Background()
	table := pgschema.MakeTableName("public", "orders")
	writer := benchmarkRedisDynamicWriter([]redisconfig.RedisCommandConfig{
		{"HSET", "{%schema%}.{%table%}:{%pk%}", "{pairs:*}"},
	}, appconfig.TxCommitTimeOptions{})

	entries := make([]*redisBatchEntry, 100)
	for i := range entries {
		writeEntry := benchmarkWriteEntry()
		entries[i] = &redisBatchEntry{
			ParallelIOBatchEntry: parallelio.ParallelIOBatchEntry{
				LSN:       pglogrepl.LSN(100 + i),
				XID:       uint32(200 + i),
				TableName: table,
				PK:        writeEntry.PK,
				Offset:    uint32(i),
			},
			walEntry:   writeEntry,
			commitTime: time.Unix(1_700_000_000, 123),
		}
	}

	buffer := &redisBatchBuffer{
		logger:      zap.NewNop(),
		redisClient: benchmarkPipelineClient{pipeline: &benchmarkPipeliner{}},
		writersMap:  map[string]redisPipelineWriter{table.FullName: writer},
		entries:     entries,
	}
	req := redisFlushRequest{
		responseTracker: discardResponseTracker{},
		stats:           stats.NewRedisStats("benchmark", []string{table.FullName}),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := buffer.flushBatch(ctx, req); err != nil {
			b.Fatalf("flushBatch failed: %v", err)
		}
	}
}

func benchmarkRedisDynamicWriter(commands []redisconfig.RedisCommandConfig, txOpts appconfig.TxCommitTimeOptions) *redisDynamicWriter {
	resolvedCommands := make([][]string, len(commands))
	for i, cmd := range commands {
		resolvedCommands[i] = []string(cmd)
	}

	cfg := &redisconfig.RedisTableConfig{
		FullName:         "public.orders",
		ResolvedCommands: []redisconfig.RedisCommandWithCondition{{Commands: resolvedCommands}},
	}
	writer := newTestRedisDynamicWriter(cfg)
	writer.txOpts = txOpts
	return writer
}

func benchmarkWriteEntry() *pgwal.WriteEntry {
	tuple := newTestOrderedMap("id", "status", "customer_id", "amount", "currency", "active", "attempts", "region")
	tuple.Set("id", "order-123")
	tuple.Set("status", "completed")
	tuple.Set("customer_id", "customer-456")
	tuple.Set("amount", 12345)
	tuple.Set("currency", "USD")
	tuple.Set("active", true)
	tuple.Set("attempts", 3)
	tuple.Set("region", "eu-central")

	return &pgwal.WriteEntry{
		Kind:  pgwal.Insert,
		Table: pgschema.MakeTableName("public", "orders"),
		PK:    "order-123",
		Tuple: tuple,
	}
}

var _ pg2buffer.ResponseTracker = discardResponseTracker{}
