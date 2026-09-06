package appconfig

import (
	"errors"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
)

type RetryPolicy struct {
	MaxRetries           int `json:"maxRetries"`
	MaxConnectionRetries int `json:"maxConnectionRetries"`

	InitialBackoff string  `json:"initialBackoff"`
	Multiplier     float64 `json:"multiplier"`
	Jitter         float64 `json:"jitter"`
	MaxBackoff     string  `json:"maxBackoff"`
}

var (
	errMaxRetries  = errors.New("retryPolicy.maxRetries should be >= 1")
	errInitBackOff = errors.New("retryPolicy.initialBackoff should be > 0")
	errMultiplier  = errors.New("retryPolicy.multiplier should be >= 1")
	errJitter      = errors.New("retryPolicy.jitter should be within [0.0, 1.0] range")
	errMaxBackOff  = errors.New("maxBackoff should be > 0")

	noRetryConfig = circuitbreaker.RetryConfig{
		MaxRetries:           1,
		MaxConnectionRetries: 0,
		InitialBackoff:       0,
		Multiplier:           0,
		Jitter:               0,
		MaxBackoff:           0,
	}
)

func (rp RetryPolicy) AsRetryConfig() (circuitbreaker.RetryConfig, error) {
	if rp.IsEmpty() {
		return noRetryConfig, nil
	}

	if rp.MaxRetries == 1 && rp.InitialBackoff == "" && rp.Multiplier == 0 && rp.Jitter == 0 && rp.MaxBackoff == "" {
		return noRetryConfig, nil
	}

	var resErr error
	if rp.MaxRetries <= 0 {
		resErr = errors.Join(resErr, errMaxRetries)
	}

	initBackoff, err := config.DurationOrSeconds(rp.InitialBackoff)

	if err != nil || initBackoff <= 0 {
		resErr = errors.Join(resErr, errInitBackOff)
	}

	maxBackoff, err := config.DurationOrSeconds(rp.MaxBackoff)

	if err != nil || maxBackoff <= 0 {
		resErr = errors.Join(resErr, errMaxBackOff)
	}

	if rp.Multiplier < 1 {
		resErr = errors.Join(resErr, errMultiplier)
	}

	if rp.Jitter < 0 || rp.Jitter > 1 {
		resErr = errors.Join(resErr, errJitter)
	}

	if resErr != nil {
		return circuitbreaker.RetryConfig{}, resErr
	}

	return circuitbreaker.RetryConfig{
		MaxRetries:           rp.MaxRetries,
		MaxConnectionRetries: rp.MaxConnectionRetries,
		InitialBackoff:       initBackoff,
		Multiplier:           rp.Multiplier,
		Jitter:               rp.Jitter,
		MaxBackoff:           maxBackoff,
	}, nil
}

func (rp RetryPolicy) IsEmpty() bool {
	return rp.MaxRetries == 0 && rp.MaxConnectionRetries == 0 && rp.InitialBackoff == "" &&
		rp.Multiplier == 0.0 && rp.Jitter == 0.0 && rp.MaxBackoff == ""
}
