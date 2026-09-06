package pglsntracker

import (
	"context"
	"crypto/rand"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// go test -bench . -run xxx -count 3 -benchmem -memprofile mem.out -cpuprofile cpu.out

type noStats struct {
}

func (no *noStats) WriteQueueInc() {
}
func (no *noStats) WriteQueueDec() {
}
func (no *noStats) RetryQueueInc() {
}
func (no *noStats) RetryQueueDec() {
}

type countingStats struct {
	writeQueue atomic.Int64
	retryQueue atomic.Int64
	retryInc   atomic.Int64
	retryDec   atomic.Int64
}

func (cs *countingStats) WriteQueueInc() {
	cs.writeQueue.Add(1)
}

func (cs *countingStats) WriteQueueDec() {
	cs.writeQueue.Add(-1)
}

func (cs *countingStats) RetryQueueInc() {
	cs.retryQueue.Add(1)
	cs.retryInc.Add(1)
}

func (cs *countingStats) RetryQueueDec() {
	cs.retryQueue.Add(-1)
	cs.retryDec.Add(1)
}

type writerError struct {
	err         string
	isRetryable bool
}

func (wr writerError) Error() string {
	return wr.err
}

func (wr writerError) IsRetryable() bool {
	return wr.isRetryable
}

func (wr writerError) IsDownStreamDown() bool {
	return false
}

func BenchmarkBufferedLSNTracker(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchBufferedLsnTracker(b)
	}
}

func benchBufferedLsnTracker(b *testing.B) {
	// logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()

	writes := createWrites(b)
	errCh := make(chan error)
	lsnTracker := NewLSNTracker(errCh, &noStats{}, logger)

	var maxCommitPoint pglogrepl2json.CommitPoint

	lastWrite := writes[len(writes)-1]

	expectedMaxCommitPoint := pglogrepl2json.CommitPoint{
		LSN: lastWrite.LSN,
		XID: lastWrite.XID,
	}

	goalCh := make(chan struct{})

	go func() {
		for {
			if lsnTracker.stopCtx.Err() != nil {
				return
			}

			cp := lsnTracker.MaxAcknowledged()
			if maxCommitPoint.LSN < cp.LSN {
				maxCommitPoint = cp
				if maxCommitPoint == expectedMaxCommitPoint {
					close(goalCh)
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for {
			wr, stopped := lsnTracker.queueWrapper.Pop()
			if stopped {
				return
			}

			var okEntries []*pgwal.ResponseEntry

			for _, entry := range wr.Entries {
				respEntry := &pgwal.ResponseEntry{
					PK:     entry.PK,
					Table:  entry.Table,
					LSN:    wr.LSN,
					XID:    wr.XID,
					Offset: entry.Offset,
				}

				okEntries = append(okEntries, respEntry)
			}

			lsnTracker.OnSuccess(&pgwal.Response{Entries: okEntries})
		}
	}()

	go func() {
		for _, wr := range writes {
			lsnTracker.Add(wr)
		}
	}()

	ctx, cancel := context.WithTimeout(b.Context(), 30*time.Second)
	defer cancel()

	var got bool

	select {
	case <-goalCh:
		got = true
	case <-ctx.Done():
		got = false
	}

	assert.True(b, got, "expected commit point hasn't been reached")
}

func createWrites(b *testing.B) []*pgwal.WriteRequest {
	b.Helper()

	const offsetsCount uint32 = 100000

	offsets := createOffsets(offsetsCount)
	writes := make([]*pgwal.WriteRequest, offsetsCount)

	for i, offset := range offsets {
		writes[i] = &pgwal.WriteRequest{
			Entries: []*pgwal.WriteEntry{
				{
					Tuple:     orderedmap.New(keymap.New("id")),
					PrevTuple: nil,
					PK:        strconv.Itoa(i),
					Kind:      pgwal.Insert,
					Table:     pgschema.MakeTableName("public", "testtxskiptable"),
				},
			},
			LSN:        offset.LSN,
			XID:        offset.XID,
			CommitTime: time.Now(),
		}
	}

	return writes
}

func createTwoWrites() *pgwal.WriteRequest {
	return &pgwal.WriteRequest{
		Entries: []*pgwal.WriteEntry{
			{
				Tuple:     orderedmap.New(keymap.New("id")),
				PrevTuple: nil,
				PK:        "1",
				Kind:      pgwal.Insert,
				Table:     pgschema.MakeTableName("public", "testtxskiptable"),
				Offset:    0,
			},
			{
				Tuple:     orderedmap.New(keymap.New("id")),
				PrevTuple: nil,
				PK:        "2",
				Kind:      pgwal.Insert,
				Table:     pgschema.MakeTableName("public", "testtxskiptable"),
				Offset:    1,
			},
		},
		LSN:        100,
		XID:        87,
		CommitTime: time.Now(),
	}
}

func TestTwoWritesInSameTx_OneFails(_ *testing.T) {
	logger, _ := zap.NewDevelopment()

	write := createTwoWrites()

	errCh := make(chan error)
	lsnTracker := NewLSNTrackerWithConfigAndStats(createTestRetryConfig(), errCh, &noStats{}, logger)

	// send write once
	lsnTracker.Add(write)

	go consumeTwoWritesInOneTx(logger, lsnTracker, false)

	goalCh := make(chan struct{})

	go func() {
		for {
			if lsnTracker.stopCtx.Err() != nil {
				return
			}

			cp := lsnTracker.MaxAcknowledged()
			if cp.LSN == write.LSN {
				close(goalCh)

				return
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	<-goalCh
}

func TestTwoWritesInSameTx_BothFail(_ *testing.T) {
	logger, _ := zap.NewDevelopment()

	write := createTwoWrites()

	errCh := make(chan error)
	lsnTracker := NewLSNTrackerWithConfigAndStats(createTestRetryConfig(), errCh, &noStats{}, logger)

	go consumeTwoWritesInOneTx(logger, lsnTracker, true)

	goalCh := make(chan struct{})

	go func() {
		for {
			if lsnTracker.stopCtx.Err() != nil {
				return
			}

			cp := lsnTracker.MaxAcknowledged()
			if cp.LSN == write.LSN {
				close(goalCh)

				return
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	// send write once
	lsnTracker.Add(write)

	<-goalCh
}

func TestMaxAcknowledgedAdvancesInLSNOrderWhenWritesAddedOutOfOrder(t *testing.T) {
	tracker := NewLSNTrackerWithConfigAndStats(createTestRetryConfig(), make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	first := releaseLifecycleWrite(1, &pgwal.WriteEntry{Offset: 0})
	second := releaseLifecycleWrite(2, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(second)
	tracker.Add(first)

	tracker.OnSuccess(releaseLifecycleSuccess(second, 0))
	require.Zero(t, tracker.MaxAcknowledged())

	tracker.OnSuccess(releaseLifecycleSuccess(first, 0))
	require.Equal(t, pglogrepl2json.CommitPoint{LSN: second.LSN, XID: second.XID}, tracker.MaxAcknowledged())
}

func TestLowerLSNAddedAfterHigherAckIsStillQueuedBeforeCommitPosRead(t *testing.T) {
	tracker := NewLSNTrackerWithConfigAndStats(createTestRetryConfig(), make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	first := releaseLifecycleWrite(1, &pgwal.WriteEntry{Offset: 0})
	second := releaseLifecycleWrite(2, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(second)

	wr, stopped := tracker.queueWrapper.Pop()
	require.False(t, stopped)
	require.Equal(t, second.LSN, wr.LSN)

	tracker.Add(first)
	tracker.OnSuccess(releaseLifecycleSuccess(second, 0))

	// write queue size points to the tracking queue
	// we should have 2 entries in the queue
	require.Equal(t, uint64(2), tracker.WriteQueueSize())

	wr, stopped = tracker.queueWrapper.Pop()
	require.False(t, stopped)
	require.Equal(t, first.LSN, wr.LSN)
	require.Zero(t, tracker.MaxAcknowledged())

	tracker.OnSuccess(releaseLifecycleSuccess(first, 0))
	require.Equal(t, pglogrepl2json.CommitPoint{LSN: second.LSN, XID: second.XID}, tracker.MaxAcknowledged())
}

func TestRetrySkippedWhenEntryAcknowledgedBeforeBackoff(t *testing.T) {
	tracker := NewLSNTrackerWithConfigAndStats(circuitbreaker.RetryConfig{
		MaxRetries:           3,
		MaxConnectionRetries: 3,
		InitialBackoff:       20 * time.Millisecond,
		Multiplier:           1,
		Jitter:               0,
		MaxBackoff:           20 * time.Millisecond,
	}, make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	write := releaseLifecycleWrite(10, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(write)
	_, stopped := tracker.queueWrapper.Pop()
	require.False(t, stopped)

	tracker.OnError(releaseLifecycleError(write, true, 0))
	tracker.OnSuccess(releaseLifecycleSuccess(write, 0))

	require.Eventually(t, func() bool {
		return tracker.WriteQueueSize() == 0
	}, 200*time.Millisecond, 10*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	require.Zero(t, tracker.readyQueue.Count())
}

func TestRetryTimerEnqueuesAfterBackoff(t *testing.T) {
	const backoff = 40 * time.Millisecond

	tracker := NewLSNTrackerWithConfigAndStats(retryTimerTestConfig(backoff), make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	write := releaseLifecycleWrite(20, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(write)
	original := popReadyWithin(t, tracker, 50*time.Millisecond)
	require.Equal(t, write.LSN, original.LSN)

	tracker.OnError(releaseLifecycleError(write, true, 0))
	time.Sleep(10 * time.Millisecond)
	require.Zero(t, tracker.readyQueue.Count())

	retry := popReadyWithin(t, tracker, 200*time.Millisecond)
	require.Equal(t, write.LSN, retry.LSN)
	require.Len(t, retry.Entries, 1)
	require.Equal(t, uint32(0), retry.Entries[0].Offset)
}

func TestRetryTimerRoundsDelayUp(t *testing.T) {
	const backoff = 25 * time.Millisecond

	tracker := NewLSNTrackerWithConfigAndStats(retryTimerTestConfig(backoff), make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	write := releaseLifecycleWrite(21, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(write)
	_ = popReadyWithin(t, tracker, 50*time.Millisecond)

	started := time.Now()
	tracker.OnError(releaseLifecycleError(write, true, 0))

	retry := popReadyWithin(t, tracker, 200*time.Millisecond)
	require.Equal(t, write.LSN, retry.LSN)
	require.GreaterOrEqual(t, time.Since(started), backoff)
}

func TestRetryTimerStopPreventsPendingRetry(t *testing.T) {
	const backoff = 50 * time.Millisecond

	tracker := NewLSNTrackerWithConfigAndStats(retryTimerTestConfig(backoff), make(chan error, 1), &noStats{}, zap.NewNop())

	write := releaseLifecycleWrite(22, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(write)
	_ = popReadyWithin(t, tracker, 50*time.Millisecond)
	tracker.OnError(releaseLifecycleError(write, true, 0))

	require.NoError(t, tracker.Stop())
	require.NoError(t, tracker.Stop())

	time.Sleep(2 * backoff)
	require.Zero(t, tracker.readyQueue.Count())
}

func TestRetryStatsBalancedOnTimerFire(t *testing.T) {
	const backoff = 20 * time.Millisecond

	stats := &countingStats{}
	tracker := NewLSNTrackerWithConfigAndStats(retryTimerTestConfig(backoff), make(chan error, 1), stats, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	write := releaseLifecycleWrite(23, &pgwal.WriteEntry{Offset: 0})

	tracker.Add(write)
	_ = popReadyWithin(t, tracker, 50*time.Millisecond)
	tracker.OnError(releaseLifecycleError(write, true, 0))

	require.Equal(t, int64(1), stats.retryInc.Load())
	require.Equal(t, int64(1), stats.retryQueue.Load())

	_ = popReadyWithin(t, tracker, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		return stats.retryDec.Load() == 1 && stats.retryQueue.Load() == 0
	}, 100*time.Millisecond, 5*time.Millisecond)
}

func TestMultipleRetryOffsetsSameLSNScheduledIndependently(t *testing.T) {
	const backoff = 20 * time.Millisecond

	tracker := NewLSNTrackerWithConfigAndStats(retryTimerTestConfig(backoff), make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	write := releaseLifecycleWrite(24,
		&pgwal.WriteEntry{Offset: 0},
		&pgwal.WriteEntry{Offset: 1},
	)

	tracker.Add(write)
	_ = popReadyWithin(t, tracker, 50*time.Millisecond)
	tracker.OnError(releaseLifecycleError(write, true, 0, 1))

	offsets := map[uint32]bool{}
	for range 2 {
		retry := popReadyWithin(t, tracker, 200*time.Millisecond)
		require.Equal(t, write.LSN, retry.LSN)
		require.Len(t, retry.Entries, 1)
		offsets[retry.Entries[0].Offset] = true
	}

	require.Equal(t, map[uint32]bool{0: true, 1: true}, offsets)
}

func retryTimerTestConfig(backoff time.Duration) circuitbreaker.RetryConfig {
	return circuitbreaker.RetryConfig{
		MaxRetries:           3,
		MaxConnectionRetries: 3,
		InitialBackoff:       backoff,
		Multiplier:           1,
		Jitter:               0,
		MaxBackoff:           backoff,
	}
}

type readyPopResult struct {
	write   *pgwal.WriteRequest
	stopped bool
}

func popReadyWithin(t *testing.T, tracker *PGBufferedLSNTracker, timeout time.Duration) *pgwal.WriteRequest {
	t.Helper()

	resultCh := make(chan readyPopResult, 1)
	go func() {
		write, stopped := tracker.queueWrapper.Pop()
		resultCh <- readyPopResult{
			write:   write,
			stopped: stopped,
		}
	}()

	select {
	case result := <-resultCh:
		require.False(t, result.stopped)
		require.NotNil(t, result.write)

		return result.write
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for ready write")

		return nil
	}
}

func consumeTwoWritesInOneTx(logger *zap.Logger, lsnTracker *PGBufferedLSNTracker, allFail bool) {
	// simulate a case when first write of the same tx is OK
	// and second case fails
	var okSent bool

	for {
		wr, stopped := lsnTracker.queueWrapper.Pop()
		if stopped {
			return
		}

		for i, entry := range wr.Entries {
			logger.Debug("got write with entry", zap.Uint32("xid", wr.XID), zap.Uint32("offset", entry.Offset), zap.String(pglogger.LSNParam, wr.LSN.String()))
			respEntry := &pgwal.ResponseEntry{
				PK:     entry.PK,
				Table:  entry.Table,
				LSN:    wr.LSN,
				XID:    wr.XID,
				Offset: entry.Offset,
			}

			if !allFail && !okSent {
				if i == 0 {
					logger.Debug("test: consumeTwoWrites: success", zap.Uint32("xid", wr.XID), zap.String("lsn", wr.LSN.String()),
						zap.Uint32("offset", entry.Offset),
					)

					lsnTracker.OnSuccess(&pgwal.Response{
						Entries: []*pgwal.ResponseEntry{respEntry},
					})

					okSent = true

					continue
				}
			}

			logger.Debug("test: consumeTwoWrites: error", zap.Uint32("xid", wr.XID), zap.String("lsn", wr.LSN.String()),
				zap.Uint32("offset", entry.Offset),
			)
			lsnTracker.OnError(&pgwal.ErrResponse{
				Entries: []*pgwal.ErrorResponseEntry{
					{
						ResponseEntry: *respEntry,
						Reason: writerError{
							err:         "test error",
							isRetryable: true,
						},
					},
				},
			})
		}
	}
}

// This test is meant to test MaxRetries setting of the buffered LSN tracker
// by default it's set to 5
// in this case LSN tracker assumes that entries will be sent by the PG again
// this is not recommended for production use.
func TestFailedEntriesSkippedEventually(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	const offsetsCount = 5

	offsets := createOffsets(offsetsCount)
	writes := createWritesFromOffsets(offsets)

	const failedOffsetIndex = offsetsCount / 2
	failedEntry := writes[failedOffsetIndex]
	logger.Debug("expected failed entry", zap.String("lsn", failedEntry.LSN.String()), zap.Uint32("xid", failedEntry.XID))

	errCh := make(chan error)
	lsnTracker := NewLSNTracker(errCh, &noStats{}, logger)

	const maxRetries = 5

	retriesMap := make(map[pglogrepl.LSN]int)

	go consumeWriteRequest(lsnTracker, logger, retriesMap, failedEntry)

	expectedMaxCommitPoint := offsets[offsetsCount-1]

	goalCtx, cancel := context.WithCancel(t.Context())

	go readFailedEntriesSkippedEventuallyCommitPoint(lsnTracker, logger, expectedMaxCommitPoint, cancel)

forLoop:
	for {
		select {
		case <-goalCtx.Done():
			break forLoop
		default:
			for _, wr := range writes {
				go func(wr *pgwal.WriteRequest) {
					logger.Debug("test: lsnTracker.Add start", zap.Uint32("xid", wr.XID))
					lsnTracker.Add(wr)
					logger.Debug("test: lsnTracker.Add end", zap.Uint32("xid", wr.XID))
				}(wr)
			}
		}

		<-time.After(10 * time.Millisecond)
	}

	var got bool

	timeoutCtx, timeoutCancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer timeoutCancel()

	select {
	case <-goalCtx.Done():
		got = true

		break
	case <-timeoutCtx.Done():
		got = false

		break
	}

	cancel()
	assert.True(t, got, "expected commit point hasn't been reached")

	for _, write := range writes {
		if write.LSN == failedEntry.LSN {
			assert.Equal(t, maxRetries, retriesMap[write.LSN], "LSN=%s XID=%d", failedEntry.LSN.String(), failedEntry.XID)
		} else {
			assert.Equal(t, 1, retriesMap[write.LSN], "LSN=%s XID=%d", write.LSN.String(), write.XID)
		}
	}
}

func readFailedEntriesSkippedEventuallyCommitPoint(lsnTracker *PGBufferedLSNTracker,
	logger *zap.Logger,
	expectedMaxCommitPoint pglogrepl2json.CommitPoint,
	cancel context.CancelFunc) {
	for {
		if lsnTracker.stopCtx.Err() != nil {
			cancel()
			return
		}
		cp := lsnTracker.MaxAcknowledged()

		if cp.LSN == expectedMaxCommitPoint.LSN {
			cancel()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func consumeWriteRequest(lsnTracker *PGBufferedLSNTracker, logger *zap.Logger,
	retriesMap map[pglogrepl.LSN]int, failedEntry *pgwal.WriteRequest) {
	for {
		wr, stopped := lsnTracker.queueWrapper.Pop()
		if stopped {
			return
		}

		var (
			okEntries  []*pgwal.ResponseEntry
			errEntries []*pgwal.ErrorResponseEntry
		)

		logger.Debug("test: got write start", zap.Uint32("xid", wr.XID))

		for _, entry := range wr.Entries {
			retriesMap[wr.LSN]++
			respEntry := &pgwal.ResponseEntry{
				PK:     entry.PK,
				Table:  entry.Table,
				LSN:    wr.LSN,
				XID:    wr.XID,
				Offset: entry.Offset,
			}

			if wr.LSN == failedEntry.LSN {
				errEntries = append(errEntries, &pgwal.ErrorResponseEntry{
					ResponseEntry: *respEntry,
					Reason: writerError{
						err:         "failed to write entry " + failedEntry.LSN.String(),
						isRetryable: true,
					},
				})
			} else {
				okEntries = append(okEntries, respEntry)
			}
		}

		if len(okEntries) > 0 {
			lsnTracker.OnSuccess(&pgwal.Response{Entries: okEntries})
		}

		if len(errEntries) > 0 {
			lsnTracker.OnError(&pgwal.ErrResponse{
				Entries: errEntries,
			})
		}

		logger.Debug("test: got write end", zap.Uint32("xid", wr.XID))
	}
}

func createTestRetryConfig() circuitbreaker.RetryConfig {
	return circuitbreaker.RetryConfig{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Millisecond,
		Multiplier:     0.5,
		Jitter:         1.1,
		MaxBackoff:     5 * time.Millisecond,
	}
}

// This test is meant to test retry technique of the buffered LSN tracker
// when provided with retryable config - the tracker will attempt to retry the failures.
func TestFailedEntriesRetriedByConfig(t *testing.T) {
	l, _ := zap.NewDevelopment()

	const offsetsCount = 5
	offsets := createOffsets(offsetsCount)
	writes := createWritesFromOffsets(offsets)

	var maxCommitPoint pglogrepl2json.CommitPoint

	errCh := make(chan error)
	tracker := NewLSNTrackerWithConfigAndStats(createTestRetryConfig(), errCh, &noStats{}, l)
	expectedMaxCommitPoint := offsets[offsetsCount-1]

	goalCh := make(chan struct{})

	go func() {
		for {
			if tracker.stopCtx.Err() != nil {
				return
			}

			cp := tracker.MaxAcknowledged()

			if cp.LSN > maxCommitPoint.LSN {
				maxCommitPoint = cp
				if maxCommitPoint.LSN == expectedMaxCommitPoint.LSN {
					close(goalCh)

					return
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	retryableFailure := writes[len(writes)/2]
	nonRetryableFailure := writes[len(writes)-1]

	retriesMap := make(map[pglogrepl.LSN]int)

	go failedEntriesConsumeFunc(tracker, retriesMap, retryableFailure, nonRetryableFailure)

	go func() {
		for _, write := range writes {
			tracker.Add(write)
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	select {
	case <-goalCh:
		break
	case <-ctx.Done():
		t.Fatal("timed out waiting for test to finish")
	}
}

func failedEntriesConsumeFunc(tracker *PGBufferedLSNTracker, retriesMap map[pglogrepl.LSN]int,
	retryableFailure *pgwal.WriteRequest, nonRetryableFailure *pgwal.WriteRequest) {
	for {
		wr, stopped := tracker.queueWrapper.Pop()
		if stopped {
			return
		}

		var (
			okEntries  []*pgwal.ResponseEntry
			errEntries []*pgwal.ErrorResponseEntry
		)

		for _, entry := range wr.Entries {
			retriesMap[wr.LSN]++
			respEntry := &pgwal.ResponseEntry{
				PK:     entry.PK,
				Table:  entry.Table,
				LSN:    wr.LSN,
				XID:    wr.XID,
				Offset: entry.Offset,
			}

			if wr.LSN == retryableFailure.LSN || wr.LSN == nonRetryableFailure.LSN {
				errEntries = append(errEntries, &pgwal.ErrorResponseEntry{
					ResponseEntry: *respEntry,
					Reason: writerError{
						err:         "failed to write entry " + wr.LSN.String(),
						isRetryable: wr.LSN == retryableFailure.LSN,
					},
				})
			} else {
				okEntries = append(okEntries, respEntry)
			}
		}

		if len(okEntries) > 0 {
			tracker.OnSuccess(&pgwal.Response{Entries: okEntries})
		}

		if len(errEntries) > 0 {
			tracker.OnError(&pgwal.ErrResponse{
				Entries: errEntries,
			})
		}
	}
}

func TestParallelLSNTracker(t *testing.T) {
	const offsetsCount = 1000
	offsets := createOffsets(offsetsCount)

	errCh := make(chan error)
	lsnTracker := NewLSNTracker(errCh, &noStats{}, zap.NewNop())

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	totalWritten := 0

	go consumeParallelLSNTracker(ctx, lsnTracker, totalWritten, offsetsCount)

	maxCommitPoint := atomic.Pointer[pglogrepl2json.CommitPoint]{}

	expectedMaxCommitPoint := offsets[offsetsCount-1]

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			cp := lsnTracker.MaxAcknowledged()
			mc := maxCommitPoint.Load()
			if mc == nil || mc.LSN < cp.LSN {
				maxCommitPoint.Store(&cp)
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	for i := 0; i < len(offsets); i++ {
		go func(i int) {
			lsnTracker.Add(&pgwal.WriteRequest{
				Entries: []*pgwal.WriteEntry{
					{
						Tuple:     orderedmap.New(keymap.New("id")),
						PrevTuple: nil,
						PK:        strconv.Itoa(i),
						Kind:      pgwal.Insert,
						Table:     pgschema.MakeTableName("public", "testtable"),
					},
				},
				LSN:        offsets[i].LSN,
				XID:        offsets[i].XID,
				CommitTime: time.Time{},
			})

			timeSleep, _ := rand.Int(rand.Reader, big.NewInt(100))

			time.Sleep(time.Duration(timeSleep.Int64()) * time.Millisecond)
		}(i)
	}

	<-ctx.Done()

	mcp := maxCommitPoint.Load()
	assert.Equal(t, expectedMaxCommitPoint, *mcp)
}

func consumeParallelLSNTracker(ctx context.Context, lsnTracker *PGBufferedLSNTracker, totalWritten int, offsetsCount int) {
	for {
		writeReq, stopped := lsnTracker.queueWrapper.Pop()
		if stopped {
			return
		}

		// mirror success back to LSN tracker
		respEntries := make([]*pgwal.ResponseEntry, len(writeReq.Entries))
		for i, entry := range writeReq.Entries {
			respEntries[i] = &pgwal.ResponseEntry{
				PK:     entry.PK,
				Table:  entry.Table,
				LSN:    writeReq.LSN,
				XID:    writeReq.XID,
				Offset: entry.Offset,
			}
		}

		totalWritten += len(writeReq.Entries)
		lsnTracker.OnSuccess(&pgwal.Response{
			Entries: respEntries,
		})

		if totalWritten == offsetsCount {
			return
		}
	}

}

func createOffsets(offsetsCount uint32) []pglogrepl2json.CommitPoint {
	offsets := make([]pglogrepl2json.CommitPoint, offsetsCount)

	var offset uint64 = 144
	for i := 0; i < len(offsets); i++ {
		offsets[i] = pglogrepl2json.CommitPoint{
			LSN: pglogrepl.LSN(offset),
			XID: uint32(i), //nolint:gosec
		}
		offset += uint64(139)
	}

	return offsets
}

func createWritesFromOffsets(offsets []pglogrepl2json.CommitPoint) []*pgwal.WriteRequest {
	writes := make([]*pgwal.WriteRequest, len(offsets))
	for i, offset := range offsets {
		writes[i] = &pgwal.WriteRequest{
			Entries: []*pgwal.WriteEntry{
				{
					Tuple:     orderedmap.New(keymap.New("id")),
					PrevTuple: nil,
					PK:        strconv.Itoa(i),
					Kind:      pgwal.Insert,
					Table:     pgschema.MakeTableName("public", "testretrytable"),
				},
			},
			LSN:        offset.LSN,
			XID:        offset.XID,
			CommitTime: time.Now(),
		}
	}

	return writes
}

func TestSameWrite(t *testing.T) {
	shutdownChan := make(chan error)
	retryConfig := circuitbreaker.RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 1 * time.Millisecond,
		Multiplier:     1,
		Jitter:         0.5,
		MaxBackoff:     5 * time.Millisecond,
	}

	tr := NewLSNTrackerWithConfigAndStats(retryConfig, shutdownChan, &noStats{}, zap.NewNop())

	writeEntry := &pgwal.WriteEntry{
		Tuple:     orderedmap.New(keymap.New("id")),
		PrevTuple: nil,
		Table:     pgschema.MakeTableName("public", "testsamewrite"),
		PK:        "1",
		Kind:      pgwal.Insert,
		Offset:    0,
	}

	write := &pgwal.WriteRequest{
		LSN:     pglogrepl.LSN(1),
		XID:     1,
		Entries: []*pgwal.WriteEntry{writeEntry},
	}

	tr.Add(write)

	errResp := &pgwal.ErrResponse{
		Entries: []*pgwal.ErrorResponseEntry{
			{
				ResponseEntry: pgwal.ResponseEntry{
					PK:     writeEntry.PK,
					Table:  writeEntry.Table,
					LSN:    write.LSN,
					XID:    write.XID,
					Offset: writeEntry.Offset,
				},
				Reason: writerError{
					err:         "test error",
					isRetryable: true,
				},
			},
		},
	}

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		tr.OnError(errResp)
		tr.OnError(errResp)
		wg.Done()
	}()

	wg.Wait()

	tr.Add(write)

	go func() {
		tr.OnError(errResp)
	}()

	testEndCh := make(chan error)

	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
		defer cancel()

		select {
		case <-t.Context().Done():
			testEndCh <- t.Context().Err()

			return
		case err := <-shutdownChan:
			testEndCh <- err

			return

		case <-ctx.Done():
			testEndCh <- nil

			return
		}
	}()

	err := <-testEndCh
	require.NoError(t, err)
}
