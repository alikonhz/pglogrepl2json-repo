package pg2redis

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolvePlaceholdersToSlice(t *testing.T) {
	now := time.Now()
	xid := uint32(12345)
	commitTimeName := "last_updated"

	tuple := newTestOrderedMap("id", "name", "active", "score")
	tuple.Set("id", 1)
	tuple.Set("name", "John Doe")
	tuple.Set("active", true)
	tuple.Set("score", 10.5)

	prevTuple := newTestOrderedMap("score")
	prevTuple.Set("score", 5.0)

	entry := &pgwal.WriteEntry{
		Table: pgschema.TableName{
			Schema: "public",
			Name:   "users",
		},
		PK:        "1",
		Tuple:     tuple,
		PrevTuple: prevTuple,
	}

	tests := []struct {
		name           string
		pattern        string
		commitTimeName string
		expected       []any
		isSlice        bool
	}{
		{
			name:     "Basic substitution {%table%}",
			pattern:  "prefix:{%table%}",
			expected: []any{"prefix:users"},
			isSlice:  false,
		},
		{
			name:     "Basic substitution {%schema%}",
			pattern:  "prefix:{%schema%}",
			expected: []any{"prefix:public"},
			isSlice:  false,
		},
		{
			name:     "Basic substitution {%xid%}",
			pattern:  "prefix:{%xid%}",
			expected: []any{"prefix:12345"},
			isSlice:  false,
		},
		{
			name:     "Basic substitution {%pk%}",
			pattern:  "prefix:{%pk%}",
			expected: []any{"prefix:1"},
			isSlice:  false,
		},
		{
			name:     "Column substitution {id}",
			pattern:  "user:{id}",
			expected: []any{"user:1"},
			isSlice:  false,
		},
		{
			name:     "Column substitution with spaces { name }",
			pattern:  "user:{ name }",
			expected: []any{"user:John Doe"},
			isSlice:  false,
		},
		{
			name:     "Boolean stringification {active}",
			pattern:  "active:{active}",
			expected: []any{"active:1"},
			isSlice:  false,
		},
		{
			name:    "Pairs wildcard {pairs:*}",
			pattern: "{pairs:*}",
			expected: []any{
				"id", "1",
				"name", "John Doe",
				"active", "1",
				"score", "10.5",
			},
			isSlice: true,
		},
		{
			name:           "Pairs wildcard with commit time {pairs:*}",
			pattern:        "{pairs:*}",
			commitTimeName: commitTimeName,
			expected: []any{
				"id", "1",
				"name", "John Doe",
				"active", "1",
				"score", "10.5",
				commitTimeName, now.Format(time.RFC3339Nano),
			},
			isSlice: true,
		},
		{
			name:    "Pairs specific {pairs:id,name}",
			pattern: "{pairs:id,name}",
			expected: []any{
				"id", "1",
				"name", "John Doe",
			},
			isSlice: true,
		},
		{
			name:    "Columns wildcard {columns:*}",
			pattern: "{columns:*}",
			expected: []any{
				"id", "name", "active", "score",
			},
			isSlice: true,
		},
		{
			name:           "Columns wildcard with commit time {columns:*}",
			pattern:        "{columns:*}",
			commitTimeName: commitTimeName,
			expected: []any{
				"id", "name", "active", "score", commitTimeName,
			},
			isSlice: true,
		},
		{
			name:    "JSON wildcard {json:*}",
			pattern: "{json:*}",
			expected: []any{
				`{"id":1,"name":"John Doe","active":true,"score":10.5}`,
			},
			isSlice: false,
		},
		{
			name:    "JSON specific {json:id,name}",
			pattern: "{json:id,name}",
			expected: []any{
				`{"id":1,"name":"John Doe"}`,
			},
			isSlice: false,
		},
		{
			name:    "Diff {diff:score}",
			pattern: "{diff:score}",
			expected: []any{
				5.5,
			},
			isSlice: false,
		},
		{
			name:    "Literal braces {{ }}",
			pattern: "literal:{{id}}",
			expected: []any{
				"literal:{id}",
			},
			isSlice: false,
		},
		{
			name:    "Mixed substitution",
			pattern: "{%schema%}.{%table%}:{id}:{name}",
			expected: []any{
				"public.users:1:John Doe",
			},
			isSlice: false,
		},
		{
			name:    "Unknown placeholder",
			pattern: "{unknown}",
			expected: []any{
				"{unknown}",
			},
			isSlice: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// If it's a JSON test, we might need to compare JSON strings which can have different order if not using ordered map
			// But resolvePlaceholdersToSlice uses ordered map for specific JSON and the entry itself has an ordered map.

			// For JSON wildcard, if saveCommitTime is true, it modifies the entry.Tuple
			// We should clone or reset if needed, but here we don't use saveCommitTime for JSON wildcard in tests yet.

			res, isSlice, err := resolvePlaceholdersToSlice(tt.pattern, entry, now, xid, appconfig.NewTxCommitTimeOptions(tt.commitTimeName))
			require.NoError(t, err)
			assert.Equal(t, tt.isSlice, isSlice)

			if tt.pattern == "{json:*}" || (strings.HasPrefix(tt.pattern, "{json:") && !tt.isSlice) {
				// Compare JSON semantically if needed, but resolvePlaceholdersToSlice uses ordered map so it should be deterministic.
				assert.JSONEq(t, tt.expected[0].(string), res[0].(string))
			} else {
				assert.Equal(t, tt.expected, res)
			}
		})
	}
}

func resolvePlaceholdersToSlice(pattern string,
	entry *pgwal.WriteEntry,
	commitTime time.Time,
	xid uint32,
	txTimeOpts appconfig.TxCommitTimeOptions) ([]any, bool, error) {
	compiled, _ := compileCommand(pattern)
	res, err := runCompiledCommand(compiled, entry, commitTime, xid, txTimeOpts)
	return res, commandExpandsToSlice(pattern), err
}

func runCompiledCommand(compiled redisCompiledCommand,
	entry *pgwal.WriteEntry,
	commitTime time.Time,
	xid uint32,
	txTimeOpts appconfig.TxCommitTimeOptions) ([]any, error) {
	requiredLen, err := compiled(true, nil, 0, entry, commitTime, xid, txTimeOpts)
	if err != nil {
		return nil, err
	}

	res := make([]any, requiredLen)
	next, err := compiled(false, res, 0, entry, commitTime, xid, txTimeOpts)
	if err != nil {
		return nil, err
	}

	return res[:next], nil
}

func commandExpandsToSlice(pattern string) bool {
	if pattern == "{pairs:*}" || pattern == "{columns:*}" {
		return true
	}

	return strings.HasPrefix(pattern, "{pairs:") && strings.HasSuffix(pattern, "}")
}

func TestResolvePlaceholdersToSlice_JsonWildcardCommitTime(t *testing.T) {
	now := time.Now()
	commitTimeName := "ct"
	tuple := newTestOrderedMap("a", commitTimeName)
	tuple.Set("a", 1)
	entry := &pgwal.WriteEntry{Tuple: tuple}

	res, isSlice, err := resolvePlaceholdersToSlice("{json:*}", entry, now, 0, appconfig.NewTxCommitTimeOptions(commitTimeName))
	require.NoError(t, err)
	assert.False(t, isSlice)

	var m map[string]any
	err = json.Unmarshal([]byte(res[0].(string)), &m)
	require.NoError(t, err)

	assert.Equal(t, float64(1), m["a"])
	assert.Equal(t, now.Format(time.RFC3339Nano), m[commitTimeName])
}

func TestResolvePlaceholdersToSlice_JsonSubsetUsesRequestedColumnOrder(t *testing.T) {
	tuple := newTestOrderedMap("id", "name")
	tuple.Set("id", 1)
	tuple.Set("name", "John Doe")
	entry := &pgwal.WriteEntry{Tuple: tuple}

	res, isSlice, err := resolvePlaceholdersToSlice("{json:name,id}", entry, time.Time{}, 0, appconfig.TxCommitTimeOptions{})
	require.NoError(t, err)
	require.False(t, isSlice)
	require.Equal(t, []any{`{"name":"John Doe","id":1}`}, res)
}

func TestResolvePlaceholdersToSlice_JsonReturnsMarshalError(t *testing.T) {
	tuple := newTestOrderedMap("bad")
	tuple.Set("bad", make(chan int))
	entry := &pgwal.WriteEntry{Tuple: tuple}

	res, isSlice, err := resolvePlaceholdersToSlice("{json:*}", entry, time.Time{}, 0, appconfig.TxCommitTimeOptions{})
	require.Error(t, err)
	require.Nil(t, res)
	require.False(t, isSlice)
}

func TestResolvePlaceholdersToSlice_JsonSubsetReturnsMarshalError(t *testing.T) {
	tuple := newTestOrderedMap("bad")
	tuple.Set("bad", func() {})
	entry := &pgwal.WriteEntry{Tuple: tuple}

	res, isSlice, err := resolvePlaceholdersToSlice("{json:bad}", entry, time.Time{}, 0, appconfig.TxCommitTimeOptions{})
	require.Error(t, err)
	require.Nil(t, res)
	require.False(t, isSlice)
}

func TestCompiledCommandUsesCurrentEntry(t *testing.T) {
	compiled, err := compileCommand("user:{id}")
	require.NoError(t, err)

	firstTuple := newTestOrderedMap("id")
	firstTuple.Set("id", 1)
	firstEntry := &pgwal.WriteEntry{Tuple: firstTuple}

	secondTuple := newTestOrderedMap("id")
	secondTuple.Set("id", 2)
	secondEntry := &pgwal.WriteEntry{Tuple: secondTuple}

	first, err := runCompiledCommand(compiled, firstEntry, time.Time{}, 0, appconfig.TxCommitTimeOptions{})
	require.NoError(t, err)
	require.Equal(t, []any{"user:1"}, first)

	second, err := runCompiledCommand(compiled, secondEntry, time.Time{}, 0, appconfig.TxCommitTimeOptions{})
	require.NoError(t, err)
	require.Equal(t, []any{"user:2"}, second)
}

func TestStringifyForRedisIntegerWidths(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "int", value: int(-1), want: "-1"},
		{name: "int8", value: int8(-2), want: "-2"},
		{name: "int16", value: int16(-3), want: "-3"},
		{name: "int32", value: int32(-4), want: "-4"},
		{name: "int64", value: int64(-5), want: "-5"},
		{name: "uint", value: uint(1), want: "1"},
		{name: "uint8", value: uint8(2), want: "2"},
		{name: "uint16", value: uint16(3), want: "3"},
		{name: "uint32", value: uint32(4), want: "4"},
		{name: "uint64", value: uint64(5), want: "5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, stringifyForRedis(tt.value))
		})
	}
}
