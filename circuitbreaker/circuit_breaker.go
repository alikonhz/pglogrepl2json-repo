package circuitbreaker

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"time"
)

type CircuitState int8

var (
	ErrOpened = errors.New("circuit open")
)

const (
	defaultClosedMaxErrors     = 5
	defaultHalfOpenedMaxErrors = 10
	defaultMaxErrorsInterval   = 500 * time.Millisecond
	defaultMaxWaitInterval     = 5 * time.Second
)

// Config defines CircuitBreaker (cb) configuration.
// ClosedMaxErrors - how many errors should _cb_ wait before transiting to the half-opened state.
// HalfOpenedMaxErrors - how many errors should _cb_ wait before transiting to the opened state.
type Config struct {
	ClosedMaxErrors     int
	HalfOpenedMaxErrors int
	MaxErrorsInterval   time.Duration
	MaxWaitInterval     time.Duration
}

func NewDefaultConfig() *Config {
	return &Config{
		ClosedMaxErrors:     defaultClosedMaxErrors,
		HalfOpenedMaxErrors: defaultHalfOpenedMaxErrors,
		MaxErrorsInterval:   defaultMaxErrorsInterval,
		MaxWaitInterval:     defaultMaxWaitInterval,
	}
}

func NewProdConfig() *Config {
	return &Config{
		ClosedMaxErrors:     defaultClosedMaxErrors,
		HalfOpenedMaxErrors: defaultHalfOpenedMaxErrors,
		MaxErrorsInterval:   1000 * time.Millisecond,
		MaxWaitInterval:     20 * time.Second,
	}
}

const (
	StateClosed CircuitState = iota
	StateOpened
	StateHalfOpened
)

type ExecFunc func(ctx context.Context, f func(ctx context.Context) error) error

type CircuitBreaker struct {
	state       CircuitState
	exec        ExecFunc
	errorsCount int
	firstErr    time.Time
	nextAttempt time.Time
	logger      *zap.Logger
	cfg         *Config
}

func New(cfg *Config) *CircuitBreaker {
	return NewWithLogger(cfg, zap.NewNop())
}

func NewWithLogger(cfg *Config, logger *zap.Logger) *CircuitBreaker {
	cb := &CircuitBreaker{
		state:       StateClosed,
		firstErr:    time.Time{},
		nextAttempt: time.Time{},
		logger:      logger,
		cfg:         cfg,
	}

	cb.state = StateClosed
	cb.exec = cb.executeInClosed

	return cb
}

func (cb *CircuitBreaker) Run(ctx context.Context, f func(ctx context.Context) error) error {
	return cb.exec(ctx, f)
}

func (cb *CircuitBreaker) executeInClosed(ctx context.Context, f func(context.Context) error) error {
	err := f(ctx)
	if err != nil {
		cb.errorInClosed()
		return err
	}

	return nil
}

func (cb *CircuitBreaker) executeInHalfOpen(ctx context.Context, f func(context.Context) error) error {
	err := cb.waitUntilNextAttempt(ctx)
	if err != nil {
		return err
	}

	err = f(ctx)
	if err != nil {
		cb.errorInHalfOpen()

		return err
	}

	cb.transitToClosed()

	return nil
}

func (cb *CircuitBreaker) executeInOpened(ctx context.Context, f func(context.Context) error) error {
	err := cb.waitUntilNextAttempt(ctx)
	if err != nil {
		return err
	}

	err = f(ctx)

	if err != nil {
		cb.incErrCount()
		cb.setMaxNextAttempt()
		cb.logger.Debug("error in opened state",
			zap.Int("errCount", cb.errorsCount),
			zap.Time("firstErr", cb.firstErr),
			zap.Time("nextAttempt", cb.nextAttempt),
		)

		return errors.Join(ErrOpened, err)
	}

	cb.transitToHalfOpened(time.Now())

	return nil
}

func (cb *CircuitBreaker) errorInClosed() {
	cb.incErrCount()
	cb.logger.Debug("error in closed state", zap.Int("errCount", cb.errorsCount), zap.Time("firstErr", cb.firstErr))

	now := time.Now()

	if cb.firstErr.IsZero() {
		cb.firstErr = now

		return
	}

	elapsed := now.Sub(cb.firstErr)
	if elapsed > cb.cfg.MaxWaitInterval {
		cb.logger.Debug("reset: elapsed > maxWaitInterval", zap.Duration("elapsed", elapsed))
		cb.reset()

		return
	}

	if cb.errorsCount >= cb.cfg.ClosedMaxErrors {
		cb.transitToHalfOpened(now)
	}
}

func (cb *CircuitBreaker) incErrCount() {
	cb.errorsCount++
}

func (cb *CircuitBreaker) reset() {
	cb.errorsCount = 0
	cb.firstErr = time.Time{}
	cb.nextAttempt = time.Time{}
}

func (cb *CircuitBreaker) incNextAttempt(now time.Time) {
	cb.nextAttempt = now.Add(cb.cfg.MaxErrorsInterval * 2)
}

func (cb *CircuitBreaker) transitToHalfOpened(now time.Time) {
	cb.state = StateHalfOpened
	cb.incNextAttempt(now)
	cb.exec = cb.executeInHalfOpen
	cb.logger.Debug("transited to half opened", zap.Time("nextAttempt", cb.nextAttempt))
}

func (cb *CircuitBreaker) transitToClosed() {
	cb.state = StateClosed
	cb.reset()
	cb.exec = cb.executeInClosed
	cb.logger.Debug("transited to closed")
}

func (cb *CircuitBreaker) transitToOpened() {
	cb.state = StateOpened
	cb.setMaxNextAttempt()
	cb.exec = cb.executeInOpened
	cb.logger.Debug("transited to opened", zap.Time("nextAttempt", cb.nextAttempt))
}

func (cb *CircuitBreaker) setMaxNextAttempt() {
	cb.nextAttempt = time.Now().Add(cb.cfg.MaxWaitInterval)
}

func (cb *CircuitBreaker) errorInHalfOpen() {
	cb.incErrCount()
	cb.logger.Debug("error in closed state", zap.Int("errCount", cb.errorsCount), zap.Time("firstErr", cb.firstErr))

	if cb.errorsCount >= cb.cfg.HalfOpenedMaxErrors {
		cb.transitToOpened()

		return
	}

	cb.incNextAttempt(time.Now())
}

func (cb *CircuitBreaker) waitUntilNextAttempt(ctx context.Context) error {
	now := time.Now()
	if now.Before(cb.nextAttempt) {
		d := cb.nextAttempt.Sub(now)
		tm := time.NewTimer(d)

		cb.logger.Debug("waiting until next attempt", zap.Time("nextAttempt", cb.nextAttempt))

		select {
		case <-ctx.Done():
			if !tm.Stop() {
				<-tm.C
			}
			return ctx.Err()
		case <-tm.C:
			break
		}
	}

	return nil
}
