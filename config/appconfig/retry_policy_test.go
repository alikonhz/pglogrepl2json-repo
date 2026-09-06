package appconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultNoRetry(t *testing.T) {
	rp := RetryPolicy{}
	rc, err := rp.AsRetryConfig()
	require.NoError(t, err)
	assert.Equal(t, noRetryConfig, rc)
}

func TestRetryPolicyIsEmpty(t *testing.T) {
	rp := RetryPolicy{}
	assert.True(t, rp.IsEmpty())
}

func TestInvalidRetryPolicy(t *testing.T) {
	rp := RetryPolicy{
		MaxRetries:     -1,
		InitialBackoff: "asd",
		MaxBackoff:     "efg",
		Multiplier:     0.8,
		Jitter:         1.2,
	}

	_, err := rp.AsRetryConfig()
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, errMaxRetries)
	assert.ErrorIs(t, err, errInitBackOff)
	assert.ErrorIs(t, err, errMaxBackOff)
	assert.ErrorIs(t, err, errMultiplier)
	assert.ErrorIs(t, err, errJitter)
}

func TestInvalidValues(t *testing.T) {
	tests := []struct {
		name          string
		rp            RetryPolicy
		expectedError error
	}{
		{"max retries < 0", withMaxRetries(-1), errMaxRetries},
		{"initial backoff = 0", withInitialBackoff("0"), errInitBackOff},
		{"initial backoff < 0", withInitialBackoff("-1"), errInitBackOff},
		{"max backoff = 0", withMaxBackoff("0"), errMaxBackOff},
		{"max backoff < 0", withMaxBackoff("-1"), errMaxBackOff},
		{"multiplier < 1", withMultiplier(0.9), errMultiplier},
		{"jitter < 0", withJitter(-0.1), errJitter},
		{"jitter > 1", withJitter(1.1), errJitter},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.rp.AsRetryConfig()
			assert.ErrorIs(t, err, test.expectedError)
		})

	}
}

func withMaxRetries(maxRetries int) RetryPolicy {
	return RetryPolicy{
		MaxRetries:           maxRetries,
		MaxConnectionRetries: 5,
		InitialBackoff:       "1",
		Multiplier:           1.1,
		Jitter:               0.6,
		MaxBackoff:           "5",
	}
}

func withInitialBackoff(initialBackoff string) RetryPolicy {
	return RetryPolicy{
		MaxRetries:           10,
		MaxConnectionRetries: 5,
		InitialBackoff:       initialBackoff,
		Multiplier:           1.1,
		Jitter:               0.6,
		MaxBackoff:           "5",
	}
}

func withMaxBackoff(maxBackoff string) RetryPolicy {
	return RetryPolicy{
		MaxRetries:           10,
		MaxConnectionRetries: 5,
		InitialBackoff:       "1",
		Multiplier:           1.1,
		Jitter:               0.6,
		MaxBackoff:           maxBackoff,
	}
}

func withMultiplier(multiplier float64) RetryPolicy {
	return RetryPolicy{
		MaxRetries:           10,
		MaxConnectionRetries: 5,
		InitialBackoff:       "1",
		Multiplier:           multiplier,
		Jitter:               0.6,
		MaxBackoff:           "5",
	}
}

func withJitter(jitter float64) RetryPolicy {
	return RetryPolicy{
		MaxRetries:           10,
		MaxConnectionRetries: 5,
		InitialBackoff:       "1",
		Multiplier:           1.1,
		Jitter:               jitter,
		MaxBackoff:           "5",
	}
}
