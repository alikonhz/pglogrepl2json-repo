package config

import (
	"strconv"
	"time"
)

func DurationOrSeconds(str string) (time.Duration, error) {
	return durationOr(str, time.Second)
}

func DurationOrMilliseconds(str string) (time.Duration, error) {
	return durationOr(str, time.Millisecond)
}

func durationOr(str string, baseDuration time.Duration) (time.Duration, error) {
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		return time.Duration(val) * baseDuration, nil
	}

	return time.ParseDuration(str)
}
