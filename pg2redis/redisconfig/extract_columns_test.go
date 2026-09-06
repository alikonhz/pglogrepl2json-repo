package redisconfig

import (
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExtractColumns(t *testing.T) {
	t.Run("extract from commands and conditions", func(t *testing.T) {
		tc := &RedisTableConfig{
			Commands: []any{
				[]string{"HSET", "orders:meta:{id}", "customer_id", "{customer_id}"},
				map[string]any{
					"command": []string{"HSET", "orders:history:{id}", "state", "{state}"},
					"condition": map[string]any{
						"op":     "=",
						"column": "state",
						"value":  "{old:prev_state}",
					},
				},
			},
		}
		// populate ResolvedCommands
		tc.ResolvedCommands = resolveCommands(tc.Commands)

		cols := tc.ExtractColumns()
		assert.Contains(t, cols, "id")
		assert.Contains(t, cols, "customer_id")
		assert.Contains(t, cols, "state")
		assert.Contains(t, cols, "prev_state")
		assert.Equal(t, 4, len(cols))
	})

	t.Run("extract from nested operation configs", func(t *testing.T) {
		tc := &RedisTableConfig{
			Insert: &TableOperationConfig{
				Commands: []any{
					[]string{"HSET", "orders:{id}", "ins_col", "{ins_col}"},
				},
			},
			Update: &TableOperationConfig{
				Commands: []any{
					map[string]any{
						"command": []string{"HSET", "orders:{id}", "upd_col", "{upd_col}"},
						"condition": map[string]any{
							"op":     "in",
							"column": "status",
							"values": []string{"{old:old_status}", "active"},
						},
					},
				},
			},
		}
		tc.InsertCommands = resolveCommands(tc.Insert.Commands)
		tc.UpdateCommands = resolveCommands(tc.Update.Commands)

		cols := tc.ExtractColumns()
		assert.Contains(t, cols, "id")
		assert.Contains(t, cols, "ins_col")
		assert.Contains(t, cols, "upd_col")
		assert.Contains(t, cols, "status")
		assert.Contains(t, cols, "old_status")
		assert.Equal(t, 5, len(cols))
	})

	t.Run("handles pairs:* correctly", func(t *testing.T) {
		tc := &RedisTableConfig{
			Commands: []any{
				[]string{"HSET", "orders:{id}", "{pairs:*}"},
			},
		}
		tc.ResolvedCommands = resolveCommands(tc.Commands)

		cols := tc.ExtractColumns()
		assert.Contains(t, cols, config.AllColumns)
		assert.Contains(t, cols, "id")
		assert.Equal(t, 2, len(cols))
	})

	t.Run("handles pairs:col1,col2 correctly", func(t *testing.T) {
		tc := &RedisTableConfig{
			Commands: []any{
				[]string{"HSET", "orders:{id}", "{pairs:col1, col2}"},
			},
		}
		tc.ResolvedCommands = resolveCommands(tc.Commands)

		cols := tc.ExtractColumns()
		assert.Contains(t, cols, "id")
		assert.Contains(t, cols, "col1")
		assert.Contains(t, cols, "col2")
		assert.Equal(t, 3, len(cols))
	})

	t.Run("handles json:* correctly", func(t *testing.T) {
		tc := &RedisTableConfig{
			Commands: []any{
				[]string{"HSET", "orders:{id}", "{json:*}"},
			},
		}
		tc.ResolvedCommands = resolveCommands(tc.Commands)

		cols := tc.ExtractColumns()
		assert.Contains(t, cols, config.AllColumns)
		assert.Contains(t, cols, "id")
		assert.Equal(t, 2, len(cols))
	})

	t.Run("handles pairs:* and condition correctly", func(t *testing.T) {
		tc := &RedisTableConfig{
			Commands: []any{
				map[string]any{
					"command": []string{"HSET", "orders:{id}", "{pairs:*}"},
					"condition": map[string]any{
						"op":     "=",
						"column": "state",
						"value":  "active",
					},
				},
			},
		}
		tc.ResolvedCommands = resolveCommands(tc.Commands)

		cols := tc.ExtractColumns()
		// Current behavior returns [config.AllColumns]
		// Desired behavior should return [config.AllColumns, "id", "state"] (order doesn't matter)
		assert.Contains(t, cols, config.AllColumns)
		assert.Contains(t, cols, "id")
		assert.Contains(t, cols, "state")
		assert.Equal(t, 3, len(cols))
	})
}
