package circuitbreaker

import (
	"math"
	"math/rand"
	"time"
)

type RetryConfig struct {
	MaxRetries           int
	MaxConnectionRetries int

	InitialBackoff time.Duration
	Multiplier     float64
	Jitter         float64
	MaxBackoff     time.Duration
}

func DefaultRetryConfig() RetryConfig {
	const defaultMaxRetries = 5

	return RetryConfig{
		MaxRetries:           defaultMaxRetries,
		MaxConnectionRetries: defaultMaxRetries,
		InitialBackoff:       5 * time.Second,
		Multiplier:           1.1,
		Jitter:               0.5,
		MaxBackoff:           20 * time.Second,
	}
}

func (fc RetryConfig) Backoff(retries int) time.Duration {
	var (
		backoff    = float64(fc.InitialBackoff)
		maxBackoff = float64(fc.MaxBackoff)
		mult       float64
		jitter     = fc.Jitter
	)

	if jitter < 0 || jitter > 1 {
		const defaultJitter = 0.5
		jitter = defaultJitter
	}

	if fc.Multiplier == 1 {
		mult = float64(retries)
	} else {
		mult = math.Pow(fc.Multiplier, float64(retries))
	}

	backoff = backoff * mult

	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	delta := backoff * jitter
	minInterval := backoff - delta
	maxInterval := backoff + delta

	return time.Duration(minInterval + (rand.Float64() * (maxInterval - minInterval)))
}
