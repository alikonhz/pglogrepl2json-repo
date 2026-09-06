package redisconfig

import (
	"testing"

	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateAgainstSchema(t *testing.T) {
	tests := []struct {
		name    string
		command RedisCommandWithCondition
		wantErr string
	}{
		{
			name: "valid condition column and old macro",
			command: RedisCommandWithCondition{
				Commands: [][]string{{"HSET", "orders:{id}", "state", "{status}"}},
				Condition: &ConditionConfig{
					Op:     OpIsDistinctFrom,
					Column: "status",
					Value:  "{old:old_status}",
				},
			},
		},
		{
			name: "missing condition column",
			command: RedisCommandWithCondition{
				Commands: [][]string{{"HSET", "orders:{id}", "state", "{status}"}},
				Condition: &ConditionConfig{
					Op:     OpEqual,
					Column: "missing",
					Value:  "active",
				},
			},
			wantErr: `condition column "missing" does not exist`,
		},
		{
			name: "missing condition macro column",
			command: RedisCommandWithCondition{
				Commands: [][]string{{"HSET", "orders:{id}", "state", "{status}"}},
				Condition: &ConditionConfig{
					Op:     OpEqual,
					Column: "status",
					Value:  "{old:missing_status}",
				},
			},
			wantErr: `condition value references column "missing_status"`,
		},
		{
			name: "missing command macro column",
			command: RedisCommandWithCondition{
				Commands: [][]string{{"HSET", "orders:{id}", "missing", "{missing_field}"}},
			},
			wantErr: `command references column "missing_field"`,
		},
		{
			name: "unsupported bool comparison",
			command: RedisCommandWithCondition{
				Commands: [][]string{{"HSET", "orders:{id}", "active", "{active}"}},
				Condition: &ConditionConfig{
					Op:     OpLess,
					Column: "active",
					Value:  "true",
				},
			},
			wantErr: `operator < is not supported for column "active"`,
		},
		{
			name: "unsupported json equality",
			command: RedisCommandWithCondition{
				Commands: [][]string{{"HSET", "orders:{id}", "metadata", "{metadata}"}},
				Condition: &ConditionConfig{
					Op:     OpEqual,
					Column: "metadata",
					Value:  "{}",
				},
			},
			wantErr: `operator = is not supported for column "metadata"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &RedisAppConfig{
				Tables: map[string]*RedisTableConfig{
					"public.orders": {
						ResolvedCommands: []RedisCommandWithCondition{tt.command},
					},
				},
			}

			err := cfg.ValidateAgainstSchema(map[string]*pgschema.Table{
				"public.orders": newSchemaValidationTestTable(),
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func newSchemaValidationTestTable() *pgschema.Table {
	table := pgschema.NewTable("public", "orders")
	table.AddColumn(&pgschema.Column{Name: "id", TypeID: pgtype.Int4OID})
	table.AddColumn(&pgschema.Column{Name: "status", TypeID: pgtype.TextOID})
	table.AddColumn(&pgschema.Column{Name: "old_status", TypeID: pgtype.TextOID})
	table.AddColumn(&pgschema.Column{Name: "active", TypeID: pgtype.BoolOID})
	table.AddColumn(&pgschema.Column{Name: "metadata", TypeID: pgtype.JSONBOID})
	return table
}
