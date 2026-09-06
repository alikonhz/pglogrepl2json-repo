package circuitbreaker

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var (
	errTest = errors.New("error")
)

func TestParallelRun(t *testing.T) {
	run := func(_ context.Context) error {
		return errTest
	}

	cb := New(newTestConfig())

	for i := 0; i < 20; i++ {
		go func() {
			cb.Run(t.Context(), run) //nolint
		}()
	}
}

func TestClosedState(t *testing.T) {
	cb := New(newTestConfig())
	called := false
	err := cb.Run(t.Context(), func(_ context.Context) error {
		called = true

		return nil
	})

	assert.Equal(t, StateClosed, cb.state)
	assert.Equal(t, 0, cb.errorsCount)

	require.NoError(t, err)
	assert.True(t, called)
}

func TestTransitionToHalfOpen(t *testing.T) {
	cb := New(newTestConfig())
	for i := 0; i < defaultClosedMaxErrors+1; i++ {
		_ = cb.Run(t.Context(), func(_ context.Context) error {
			return errTest
		})
	}

	assert.Equal(t, StateHalfOpened, cb.state)
	assert.Equal(t, defaultClosedMaxErrors+1, cb.errorsCount)
}

func TestTransitionToHalfOpenThenToClosed(t *testing.T) {
	cb := New(newTestConfig())
	for i := 0; i < defaultClosedMaxErrors+1; i++ {
		_ = cb.Run(t.Context(), func(_ context.Context) error {
			return errTest
		})
	}

	_ = cb.Run(t.Context(), func(_ context.Context) error {
		return nil
	})

	assert.Equal(t, StateClosed, cb.state)
	assert.Equal(t, 0, cb.errorsCount)
}

func TestTransitionToOpened(t *testing.T) {
	cb := New(newTestConfig())
	for i := 0; i < defaultHalfOpenedMaxErrors+1; i++ {
		_ = cb.Run(t.Context(), func(_ context.Context) error {
			return errTest
		})
	}

	assert.Equal(t, StateOpened, cb.state)
	assert.Equal(t, defaultHalfOpenedMaxErrors+1, cb.errorsCount)
}

func TestTransitionToOpenedThenToHalfClosed(t *testing.T) {
	cb := New(newTestConfig())
	for i := 0; i < defaultHalfOpenedMaxErrors+1; i++ {
		_ = cb.Run(t.Context(), func(_ context.Context) error {
			return errTest
		})
	}

	_ = cb.Run(t.Context(), func(_ context.Context) error {
		return nil
	})

	assert.Equal(t, StateHalfOpened, cb.state)
	assert.Equal(t, defaultHalfOpenedMaxErrors+1, cb.errorsCount)
}

func TestTransitionToOpenedThenToClosed(t *testing.T) {
	cb := New(newTestConfig())
	for i := 0; i < defaultHalfOpenedMaxErrors+1; i++ {
		_ = cb.Run(t.Context(), func(_ context.Context) error {
			return errTest
		})
	}

	// two times without error
	_ = cb.Run(t.Context(), func(_ context.Context) error {
		return nil
	})
	_ = cb.Run(t.Context(), func(_ context.Context) error {
		return nil
	})

	assert.Equal(t, StateClosed, cb.state)
	assert.Equal(t, 0, cb.errorsCount)
}

func newTestConfig() *Config {
	return &Config{
		ClosedMaxErrors:     defaultClosedMaxErrors,
		HalfOpenedMaxErrors: defaultHalfOpenedMaxErrors,
		MaxErrorsInterval:   150 * time.Millisecond,
		MaxWaitInterval:     1 * time.Second,
	}
}
