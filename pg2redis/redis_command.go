package pg2redis

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/pgwal"
)

var (
	placeholderRegExp = regexp.MustCompile(`\{([^{}]+)\}`)
)

type redisCompiledCommand func(eval bool,
	arr []any,
	startAt uint32,
	entry *pgwal.WriteEntry,
	commitTime time.Time,
	xid uint32,
	txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error)

func literalCommand(value string) redisCompiledCommand {
	return func(eval bool,
		arr []any,
		startAt uint32,
		entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
		if eval {
			return 1, nil
		}

		arr[startAt] = value
		return startAt + 1, nil
	}
}

func pairsStarCommand(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
	capacity := entry.Tuple.Size() * 2
	if txTimeOpts.Save {
		capacity += 2
	}

	if eval {
		return capacity, nil
	}

	for key, value := range entry.Tuple.Iter() {
		arr[startAt] = key
		arr[startAt+1] = stringifyForRedis(value)
		startAt += 2
	}

	if txTimeOpts.Save {
		arr[startAt] = txTimeOpts.Name
		arr[startAt+1] = commitTime.Format(time.RFC3339Nano)
		startAt += 2
	}

	return startAt, nil
}

func pairsSubsetCommand(cols []string) redisCompiledCommand {
	return func(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
		if eval {
			return uint32(len(cols) * 2), nil
		}

		for _, col := range cols {
			if val, ok := entry.Tuple.Get(col); ok {
				arr[startAt] = col
				arr[startAt+1] = stringifyForRedis(val)
				startAt += 2
			}
		}

		return startAt, nil
	}
}

func columnsStarCommand(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, _ time.Time, _ uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
	capacity := entry.Tuple.Size()
	if txTimeOpts.Save {
		capacity++
	}

	if eval {
		return capacity, nil
	}

	for k, _ := range entry.Tuple.Iter() {
		arr[startAt] = k
		startAt++
	}

	if txTimeOpts.Save {
		arr[startAt] = txTimeOpts.Name
		startAt++
	}

	return startAt, nil
}

func jsonStarCommand(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
	if eval {
		return 1, nil
	}

	if txTimeOpts.Save {
		entry.Tuple.Set(txTimeOpts.Name, commitTime.Format(time.RFC3339Nano))
	}

	j, err := entry.Tuple.MarshalJSON()
	if err != nil {
		return 0, err
	}

	arr[startAt] = string(j)
	return startAt + 1, nil
}

func jsonSubsetCommand(cols []string) redisCompiledCommand {
	return func(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
		if eval {
			return 1, nil
		}

		j, err := entry.Tuple.CustomMarshalJSON(cols, nil)
		if err != nil {
			return 0, err
		}

		arr[startAt] = string(j)
		return startAt + 1, nil
	}
}

func diffCommand(colName string) redisCompiledCommand {
	return func(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
		if eval {
			return 1, nil
		}

		var curVal, prevVal float64
		if val, ok := entry.Tuple.Get(colName); ok {
			curVal, _, _ = convertToFloat64(val)
		}
		if entry.PrevTuple != nil {
			if val, ok := entry.PrevTuple.Get(colName); ok {
				prevVal, _, _ = convertToFloat64(val)
			}
		}

		arr[startAt] = curVal - prevVal
		return startAt + 1, nil
	}
}

// the template part uses strings.Builder to create a final string
// the strings.Builder in a hot path allocates a lot and causes a lot of CPU to resize internal buffer,
// because we don't know the length of the final string
// also, the OrderedMap.GetIndex in a hot path is taking CPU as well
// so we can't call it more than once
// to avoid this, we will use a wrap function with redisTemplatePartFunc
// on the first call, the wrap func will call the wrapped redisTemplatePartFunc and cache the result
// the template builder will use it to calculate the final size of the string, then use it to create a strings.Builder with correct size
// and then call the function once again
// the 2nd wrap call will return the cached result
type redisTemplatePartFunc func(entry *pgwal.WriteEntry, xid uint32) string

func literalTemplatePart(value string) redisTemplatePartFunc {
	return func(entry *pgwal.WriteEntry, xid uint32) string {
		return value
	}
}

func placeholderTemplatePart(match string) redisTemplatePartFunc {
	content := match[1 : len(match)-1]

	switch content {
	case "%table%":
		return func(entry *pgwal.WriteEntry, xid uint32) string {
			return entry.Table.Name
		}
	case "%schema%":
		return func(entry *pgwal.WriteEntry, xid uint32) string {
			return entry.Table.Schema
		}
	case "%xid%":
		return wrap(func(entry *pgwal.WriteEntry, xid uint32) string {
			return strconv.FormatUint(uint64(xid), 10)
		})
	case "%pk%":
		return func(entry *pgwal.WriteEntry, xid uint32) string {
			return entry.PK
		}
	}

	colName := strings.TrimSpace(content)
	if strings.HasPrefix(colName, "old:") {
		colName = colName[4:]

		return wrap(func(entry *pgwal.WriteEntry, xid uint32) string {
			if entry.PrevTuple != nil {
				if v, ok := entry.PrevTuple.Get(colName); ok {
					return stringifyForRedis(v)
				}
			}

			return ""
		})
	}

	return wrap(func(entry *pgwal.WriteEntry, xid uint32) string {
		if v, ok := entry.Tuple.Get(colName); ok {
			return stringifyForRedis(v)
		}

		return match
	})
}

func wrap(partFunc redisTemplatePartFunc) redisTemplatePartFunc {
	return func(entry *pgwal.WriteEntry, xid uint32) string {
		return partFunc(entry, xid)
	}
}

func templateCommand(parts []redisTemplatePartFunc) redisCompiledCommand {
	return func(eval bool, arr []any, startAt uint32, entry *pgwal.WriteEntry, commitTime time.Time, xid uint32, txTimeOpts appconfig.TxCommitTimeOptions) (uint32, error) {
		if eval {
			return 1, nil
		}

		totalSize := 0
		values := make([]string, len(parts))
		for i, part := range parts {
			res := part(entry, xid)
			values[i] = res
			totalSize += len(res)
		}

		var b strings.Builder
		b.Grow(totalSize)

		for i, _ := range values {
			b.WriteString(values[i])
		}

		arr[startAt] = b.String()
		return startAt + 1, nil
	}
}

func splitPlaceholderColumns(colsStr string) []string {
	cols := strings.Split(colsStr, ",")
	for i, col := range cols {
		cols[i] = strings.TrimSpace(col)
	}

	return cols
}

func compileTemplateCommand(pattern string) redisCompiledCommand {
	matches := placeholderRegExp.FindAllStringIndex(pattern, -1)
	if len(matches) == 0 {
		return literalCommand(strings.ReplaceAll(strings.ReplaceAll(pattern, "\x00", "{"), "\x01", "}"))
	}

	parts := make([]redisTemplatePartFunc, 0, len(matches)*2+1)
	lastEnd := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		if start > lastEnd {
			literal := strings.ReplaceAll(strings.ReplaceAll(pattern[lastEnd:start], "\x00", "{"), "\x01", "}")
			parts = append(parts, literalTemplatePart(literal))
		}

		parts = append(parts, placeholderTemplatePart(pattern[start:end]))
		lastEnd = end
	}

	if lastEnd < len(pattern) {
		literal := strings.ReplaceAll(strings.ReplaceAll(pattern[lastEnd:], "\x00", "{"), "\x01", "}")
		parts = append(parts, literalTemplatePart(literal))
	}

	return templateCommand(parts)
}

func compileCommand(pattern string) (redisCompiledCommand, error) {
	if pattern == "{pairs:*}" {
		return pairsStarCommand, nil
	}

	const pairsPrefix = "{pairs:"
	const pairsPrefixLen = len(pairsPrefix)

	if strings.HasPrefix(pattern, pairsPrefix) && strings.HasSuffix(pattern, "}") && !strings.Contains(pattern[pairsPrefixLen:len(pattern)-1], "{") {
		colsStr := pattern[pairsPrefixLen : len(pattern)-1]
		cols := splitPlaceholderColumns(colsStr)
		return pairsSubsetCommand(cols), nil
	}

	if pattern == "{columns:*}" {
		return columnsStarCommand, nil
	}

	if pattern == "{json:*}" {
		return jsonStarCommand, nil
	}

	const jsonPrefix = "{json:"
	const jsonPrefixLen = len(jsonPrefix)

	if strings.HasPrefix(pattern, jsonPrefix) && strings.HasSuffix(pattern, "}") && !strings.Contains(pattern[jsonPrefixLen:len(pattern)-1], "{") {
		colsStr := pattern[jsonPrefixLen : len(pattern)-1]
		cols := splitPlaceholderColumns(colsStr)

		return jsonSubsetCommand(cols), nil
	}

	const diffPrefix = "{diff:"
	const diffPrefixLen = len(diffPrefix)

	if strings.HasPrefix(pattern, diffPrefix) && strings.HasSuffix(pattern, "}") && !strings.Contains(pattern[diffPrefixLen:len(pattern)-1], "{") {
		colName := strings.TrimSpace(pattern[diffPrefixLen : len(pattern)-1])
		return diffCommand(colName), nil
	}

	// Handle {{ and }} by replacing them with a temporary marker
	// to avoid being matched by the placeholder regex.
	// This ensures they are treated as literal characters.
	result := strings.ReplaceAll(pattern, "{{", "\x00")
	result = strings.ReplaceAll(result, "}}", "\x01")

	return compileTemplateCommand(result), nil
}

func stringifyForRedis(v any) string {
	// the stringifyForRedis function is on the hot path
	// by using a switch statement, we can avoid excess allocations and performance overhead then by using fmt.Sprintf
	switch b := v.(type) {
	case bool:
		if b {
			return "1"
		}
		return "0"
	case string:
		return b
	case int:
		return strconv.FormatInt(int64(b), 10)
	case int8:
		return strconv.FormatInt(int64(b), 10)
	case int16:
		return strconv.FormatInt(int64(b), 10)
	case int32:
		return strconv.FormatInt(int64(b), 10)
	case int64:
		return strconv.FormatInt(b, 10)
	case uint:
		return strconv.FormatUint(uint64(b), 10)
	case uint8:
		return strconv.FormatUint(uint64(b), 10)
	case uint16:
		return strconv.FormatUint(uint64(b), 10)
	case uint32:
		return strconv.FormatUint(uint64(b), 10)
	case uint64:
		return strconv.FormatUint(b, 10)
	case float32:
		return strconv.FormatFloat(float64(b), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(b, 'g', -1, 64)
	case decode.SmartFloat32:
		return strconv.FormatFloat(float64(b), 'g', -1, 32)
	case decode.SmartFloat64:
		return strconv.FormatFloat(float64(b), 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}
