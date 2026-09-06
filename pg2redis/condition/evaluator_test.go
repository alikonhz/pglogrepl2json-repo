package condition

import (
	"testing"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgwal"
)

func TestEvaluator_Evaluate(t *testing.T) {
	eval := NewEvaluator()

	tuple := orderedmap.New(keymap.New("status", "price", "active", "null_col"))
	tuple.Set("status", "A")
	tuple.Set("price", 100)
	tuple.Set("active", true)
	tuple.Set("null_col", nil)

	prevTuple := orderedmap.New(keymap.New("status", "price"))
	prevTuple.Set("status", "P")
	prevTuple.Set("price", 80)

	entry := &pgwal.WriteEntry{
		Tuple:     tuple,
		PrevTuple: prevTuple,
	}

	tests := []struct {
		name    string
		cond    *redisconfig.ConditionConfig
		want    bool
		wantErr bool
	}{
		{
			name: "equal string match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpEqual, Value: "A"},
			want: true,
		},
		{
			name: "equal string no match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpEqual, Value: "B"},
			want: false,
		},
		{
			name: "not equal string match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpNotEqual, Value: "B"},
			want: true,
		},
		{
			name: "equal numeric exact string match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpEqual, Value: "100"},
			want: true,
		},
		{
			name: "equal numeric different string form no match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpEqual, Value: "100.0"},
			want: false,
		},
		{
			name: "not equal numeric different string form match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpNotEqual, Value: "100.0"},
			want: true,
		},
		{
			name: "equal bool string match",
			cond: &redisconfig.ConditionConfig{Column: "active", Op: redisconfig.OpEqual, Value: "true"},
			want: true,
		},
		{
			name: "equal bool redis form no match",
			cond: &redisconfig.ConditionConfig{Column: "active", Op: redisconfig.OpEqual, Value: "1"},
			want: false,
		},
		{
			name: "greater numeric match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpGreater, Value: "50"},
			want: true,
		},
		{
			name: "greater numeric no match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpGreater, Value: "150"},
			want: false,
		},
		{
			name: "in list match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpIn, Values: []string{"A", "B", "C"}},
			want: true,
		},
		{
			name: "in list numeric exact string match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpIn, Values: []string{"99", "100"}},
			want: true,
		},
		{
			name: "in list numeric different string form no match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpIn, Values: []string{"99", "100.0"}},
			want: false,
		},
		{
			name: "in list no match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpIn, Values: []string{"X", "Y"}},
			want: false,
		},
		{
			name: "is null match",
			cond: &redisconfig.ConditionConfig{Column: "null_col", Op: redisconfig.OpIsNull},
			want: true,
		},
		{
			name: "is not null match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpIsNotNull},
			want: true,
		},
		{
			name: "compare with old value match",
			cond: &redisconfig.ConditionConfig{Column: "price", Op: redisconfig.OpGreater, Value: "{old:price}"},
			want: true,
		},
		{
			name: "is distinct from old value match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpIsDistinctFrom, Value: "{old:status}"},
			want: true,
		},
		{
			name: "is distinct from same value no match",
			cond: &redisconfig.ConditionConfig{Column: "status", Op: redisconfig.OpIsDistinctFrom, Value: "A"},
			want: false,
		},
		{
			name: "is distinct from null match",
			cond: &redisconfig.ConditionConfig{Column: "null_col", Op: redisconfig.OpIsDistinctFrom, Value: "A"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eval.Evaluate(tt.cond, entry)
			if (err != nil) != tt.wantErr {
				t.Errorf("Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Evaluate() got = %v, want %v", got, tt.want)
			}
		})
	}
}
