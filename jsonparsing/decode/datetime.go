package decode

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"time"
)

const (
	microsecondsPerSecond = 1000000
	microsecondsPerMinute = 60 * microsecondsPerSecond
	microsecondsPerHour   = 60 * microsecondsPerMinute
	microsecondsPerDay    = 24 * microsecondsPerHour
	microsecondsPerMonth  = 30 * microsecondsPerDay
)

func timestampDecoderToDate(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	return timestampDecodeWithFormat(data, dataType, typeMap, encoding, time.DateOnly)
}

func timestampDecoderToDateTime(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	return timestampDecodeWithFormat(data, dataType, typeMap, encoding, time.RFC3339Nano)
}

func timeDecoder(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	v, err := decodeFromTypeMap(data, dataType, typeMap)
	if err != nil {
		return nil, fmt.Errorf("failed to decode time: %w", err)
	}

	t, ok := v.(pgtype.Time)
	if !ok {
		return nil, unexpectedTypeErr("time decoder", dataType, v)
	}

	return timeToString(t), nil
}

func timestampDecodeWithFormat(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat, timeFormat string) (any, error) {

	v, err := decodeFromTypeMap(data, dataType, typeMap)
	if err != nil {
		return nil, fmt.Errorf("failed to decode timestamp/date: %w", err)
	}

	t, ok := v.(time.Time)
	if !ok {
		return nil, unexpectedTypeErr("date/timestamp decoder", dataType, v)
	}

	return t.Format(timeFormat), nil
}

func timeToString(t pgtype.Time) string {
	usec := t.Microseconds
	hours := usec / microsecondsPerHour
	usec -= hours * microsecondsPerHour
	minutes := usec / microsecondsPerMinute
	usec -= minutes * microsecondsPerMinute
	seconds := usec / microsecondsPerSecond
	usec -= seconds * microsecondsPerSecond

	s := fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, usec)

	return s
}
