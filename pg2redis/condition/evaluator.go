package condition

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgwal"
)

type Evaluator struct{}

func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

func (e *Evaluator) Evaluate(cond *redisconfig.ConditionConfig, entry *pgwal.WriteEntry) (bool, error) {
	if cond == nil {
		return true, nil
	}

	val, ok := e.resolveValue(cond.Column, entry)
	if !ok {
		// If a column doesn't exist in tuple, we should have validated this at startup.
		// At runtime, if it's missing, we treat it as NULL.
		val = nil
	}

	switch cond.Op {
	case redisconfig.OpIsNull:
		return val == nil, nil
	case redisconfig.OpIsNotNull:
		return val != nil, nil
	case redisconfig.OpIsDistinctFrom:
		otherVal, _ := e.resolveValueMacro(cond.Value, entry)
		return isDistinctFrom(val, otherVal), nil
	}

	// For all other operators, if val is NULL, it doesn't match
	if val == nil {
		return false, nil
	}

	switch cond.Op {
	case redisconfig.OpIn:
		for _, vMacro := range cond.Values {
			otherVal, _ := e.resolveValueMacro(vMacro, entry)
			if isEqual(val, otherVal) {
				return true, nil
			}
		}
		return false, nil
	case redisconfig.OpNotIn:
		for _, vMacro := range cond.Values {
			otherVal, _ := e.resolveValueMacro(vMacro, entry)
			if isEqual(val, otherVal) {
				return false, nil
			}
		}
		return true, nil
	case redisconfig.OpEqual, redisconfig.OpNotEqual, redisconfig.OpLess, redisconfig.OpGreater, redisconfig.OpLessEq, redisconfig.OpGreaterEq:
		otherVal, _ := e.resolveValueMacro(cond.Value, entry)
		if otherVal == nil {
			// SQL semantics: comparison with NULL is unknown/false
			return false, nil
		}
		return compare(val, otherVal, cond.Op)
	default:
		return false, fmt.Errorf("unsupported operator: %s", cond.Op)
	}
}

func (e *Evaluator) resolveValue(colName string, entry *pgwal.WriteEntry) (any, bool) {
	if strings.HasPrefix(colName, "{old:") && strings.HasSuffix(colName, "}") {
		inner := colName[5 : len(colName)-1]
		if entry.PrevTuple == nil {
			return nil, false
		}
		return entry.PrevTuple.Get(inner)
	}
	if strings.HasPrefix(colName, "{") && strings.HasSuffix(colName, "}") {
		inner := colName[1 : len(colName)-1]
		return entry.Tuple.Get(inner)
	}
	return entry.Tuple.Get(colName)
}

func (e *Evaluator) resolveValueMacro(macro string, entry *pgwal.WriteEntry) (any, bool) {
	if strings.HasPrefix(macro, "{old:") && strings.HasSuffix(macro, "}") {
		colName := macro[5 : len(macro)-1]
		if entry.PrevTuple == nil {
			return nil, false
		}
		return entry.PrevTuple.Get(colName)
	}
	if strings.HasPrefix(macro, "{") && strings.HasSuffix(macro, "}") {
		colName := macro[1 : len(macro)-1]
		return entry.Tuple.Get(colName)
	}
	// It's a literal value
	return macro, true
}

func isEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	return stringifyForCondition(a) == stringifyForCondition(b)
}

func isDistinctFrom(a, b any) bool {
	if a == nil && b == nil {
		return false
	}
	if a == nil || b == nil {
		return true
	}
	return !isEqual(a, b)
}

func compare(a, b any, op redisconfig.ConditionOperator) (bool, error) {
	switch op {
	case redisconfig.OpEqual:
		return isEqual(a, b), nil
	case redisconfig.OpNotEqual:
		return !isEqual(a, b), nil
	}

	af, okA := convertToFloat64(a)
	bf, okB := convertToFloat64(b)

	if okA && okB {
		switch op {
		case redisconfig.OpLess:
			return af < bf, nil
		case redisconfig.OpGreater:
			return af > bf, nil
		case redisconfig.OpLessEq:
			return af <= bf, nil
		case redisconfig.OpGreaterEq:
			return af >= bf, nil
		}
	}

	as := stringifyForCondition(a)
	bs := stringifyForCondition(b)

	switch op {
	case redisconfig.OpLess:
		return as < bs, nil
	case redisconfig.OpGreater:
		return as > bs, nil
	case redisconfig.OpLessEq:
		return as <= bs, nil
	case redisconfig.OpGreaterEq:
		return as >= bs, nil
	}

	return false, fmt.Errorf("unsupported operator for comparison: %s", op)
}

func convertToFloat64(v any) (float64, bool) {
	switch i := v.(type) {
	case int:
		return float64(i), true
	case int8:
		return float64(i), true
	case int16:
		return float64(i), true
	case int32:
		return float64(i), true
	case int64:
		return float64(i), true
	case uint:
		return float64(i), true
	case uint8:
		return float64(i), true
	case uint16:
		return float64(i), true
	case uint32:
		return float64(i), true
	case uint64:
		return float64(i), true
	case float32:
		return float64(i), true
	case float64:
		return i, true
	case decode.SmartFloat32:
		return float64(i), true
	case decode.SmartFloat64:
		return float64(i), true
	case string:
		f, err := strconv.ParseFloat(i, 64)
		return f, err == nil
	}
	return 0, false
}

func stringifyForCondition(v any) string {
	switch i := v.(type) {
	case bool:
		return strconv.FormatBool(i)
	case string:
		return i
	case int:
		return strconv.FormatInt(int64(i), 10)
	case int8:
		return strconv.FormatInt(int64(i), 10)
	case int16:
		return strconv.FormatInt(int64(i), 10)
	case int32:
		return strconv.FormatInt(int64(i), 10)
	case int64:
		return strconv.FormatInt(i, 10)
	case uint:
		return strconv.FormatUint(uint64(i), 10)
	case uint8:
		return strconv.FormatUint(uint64(i), 10)
	case uint16:
		return strconv.FormatUint(uint64(i), 10)
	case uint32:
		return strconv.FormatUint(uint64(i), 10)
	case uint64:
		return strconv.FormatUint(i, 10)
	case float32:
		return strconv.FormatFloat(float64(i), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(i, 'g', -1, 64)
	case decode.SmartFloat32:
		return strconv.FormatFloat(float64(i), 'g', -1, 32)
	case decode.SmartFloat64:
		return strconv.FormatFloat(float64(i), 'g', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}
