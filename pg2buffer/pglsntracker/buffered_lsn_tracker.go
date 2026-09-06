package pglsntracker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/alikonhz/pglogrepl2json/writequeue"
	"github.com/jackc/pglogrepl"
	"github.com/zeromicro/go-zero/core/collection"
	"go.uber.org/zap"
)

const (
	defaultRetryWheelInterval = 10 * time.Millisecond
	minRetryWheelInterval     = time.Millisecond
	retryWheelSlots           = 1024
)

type PGBufferedLSNTracker struct {
	trackingQueue *writequeue.WriteQueue[*trackedWriteRequest]
	readyQueue    *writequeue.WriteQueue[*readyWriteEntry]
	queueWrapper  writequeue.ReadonlyQueue[*pgwal.WriteRequest]

	mu              sync.Mutex
	maxAcknowledged pglogrepl2json.CommitPoint
	retryConfig     circuitbreaker.RetryConfig
	retryWheel      *collection.TimingWheel
	retryInterval   time.Duration
	retrySeq        atomic.Uint64
	startOnce       sync.Once
	startErr        error
	stopOnce        sync.Once

	coordinatedShutdownCh chan error

	stopCtx context.Context
	stop    context.CancelFunc

	stats LSNTrackerStats

	logger *zap.Logger
}

type retryEntry struct {
	trackedWrite *trackedWriteRequest
	write        *pgwal.WriteRequest
	waitUntil    time.Time
}

func (re retryEntry) Size() uint32 {
	return re.write.Size()
}

type readyWriteEntry struct {
	trackedWrite *trackedWriteRequest
	write        *pgwal.WriteRequest
}

func (e *readyWriteEntry) Size() uint32 {
	if e == nil || e.write == nil {
		return 0
	}

	return e.write.Size()
}

type trackedWriteRequest struct {
	mu sync.Mutex

	point    pglogrepl2json.CommitPoint
	write    *pgwal.WriteRequest
	size     uint32
	pending  map[uint32]struct{}
	failures map[uint32]*failureEntry
	queued   []*pgwal.WriteRequest
	released bool
}

func newTrackedWriteRequest(write *pgwal.WriteRequest) *trackedWriteRequest {
	pending := make(map[uint32]struct{}, len(write.Entries))
	if len(write.Entries) == 0 {
		pending[0] = struct{}{}
	} else {
		for _, entry := range write.Entries {
			pending[entry.Offset] = struct{}{}
		}
	}

	return &trackedWriteRequest{
		point: pglogrepl2json.CommitPoint{
			LSN: write.LSN,
			XID: write.XID,
		},
		write:   write,
		size:    write.Size(),
		pending: pending,
	}
}

func (tw *trackedWriteRequest) Size() uint32 {
	return tw.size
}

func (tw *trackedWriteRequest) queueWrite(write *pgwal.WriteRequest) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if write == nil || tw.released || !tw.hasPendingEntries(write) {
		return false
	}

	tw.queued = append(tw.queued, write)

	return true
}

func (tw *trackedWriteRequest) clearQueued() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	clear(tw.queued)
	tw.queued = nil
}

func (tw *trackedWriteRequest) popQueuedWrite() *pgwal.WriteRequest {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if len(tw.queued) == 0 {
		return nil
	}

	write := tw.queued[0]
	copy(tw.queued, tw.queued[1:])
	tw.queued[len(tw.queued)-1] = nil
	tw.queued = tw.queued[:len(tw.queued)-1]

	return write
}

func (tw *trackedWriteRequest) ack(offsets []uint32) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	for _, offset := range offsets {
		delete(tw.pending, offset)
	}

	return len(tw.pending) == 0
}

func (tw *trackedWriteRequest) isAcknowledged() bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return len(tw.pending) == 0
}

func (tw *trackedWriteRequest) hasFailures() bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return len(tw.failures) > 0
}

func (tw *trackedWriteRequest) getFailedEntry(offset uint32, reason pgwal.WriterError) *failureEntry {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.failures == nil {
		tw.failures = make(map[uint32]*failureEntry)
	}

	failEntry, exists := tw.failures[offset]
	if !exists {
		upstrDownRetries := 0
		if reason.IsDownStreamDown() {
			upstrDownRetries = 1
		}

		failEntry = &failureEntry{
			retries:               1,
			lastFailed:            time.Now(),
			downStreamDownRetries: upstrDownRetries,
		}
		tw.failures[offset] = failEntry

		return failEntry
	}

	failEntry.lastFailed = time.Now()
	if reason.IsDownStreamDown() {
		failEntry.downStreamDownRetries++
	} else {
		failEntry.retries++
	}

	return failEntry
}

func (tw *trackedWriteRequest) retryWrite(offset uint32) *pgwal.WriteRequest {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.write == nil {
		return nil
	}

	if _, pending := tw.pending[offset]; !pending {
		return nil
	}

	writeEntry := tw.write.GetEntryByOffset(offset)
	if writeEntry == nil {
		return nil
	}

	return &pgwal.WriteRequest{
		Entries: []*pgwal.WriteEntry{
			writeEntry,
		},
		LSN:        tw.write.LSN,
		XID:        tw.write.XID,
		CommitTime: tw.write.CommitTime,
	}
}

func (tw *trackedWriteRequest) fullWrite() *pgwal.WriteRequest {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.write == nil || tw.released || !tw.hasPendingEntries(tw.write) {
		return nil
	}

	return tw.write
}

func (tw *trackedWriteRequest) hasPendingEntries(write *pgwal.WriteRequest) bool {
	if write == nil || len(tw.pending) == 0 {
		return false
	}

	if len(write.Entries) == 0 {
		_, pending := tw.pending[0]

		return pending
	}

	for _, entry := range write.Entries {
		if _, pending := tw.pending[entry.Offset]; pending {
			return true
		}
	}

	return false
}

func (tw *trackedWriteRequest) release() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.released {
		return 0
	}

	queuedCount := len(tw.queued)

	if tw.write != nil {
		for _, entry := range tw.write.Entries {
			entry.Release()
		}
	}

	clear(tw.queued)
	tw.queued = nil
	tw.pending = nil
	tw.failures = nil
	tw.write = nil
	tw.released = true

	return queuedCount
}

type failureEntry struct {
	retries               int
	downStreamDownRetries int
	lastFailed            time.Time
}

func (fe *failureEntry) totalRetries() int {
	return fe.retries + fe.downStreamDownRetries - 1
}

var (
	errWriteIsMissing = errors.New("internal error: write is missing. this may indicate a bug in the product. please contact support")
)

type LSNTrackerStats interface {
	WriteQueueInc()
	WriteQueueDec()

	RetryQueueInc()
	RetryQueueDec()
}

func NewLSNTracker(coordinatedShutdownCh chan error, stats LSNTrackerStats, logger *zap.Logger) *PGBufferedLSNTracker {
	const defaultMaxRetry = 5

	return NewLSNTrackerWithConfigAndStats(circuitbreaker.RetryConfig{
		MaxRetries:           defaultMaxRetry,
		MaxConnectionRetries: defaultMaxRetry,
		InitialBackoff:       0,
		Multiplier:           0,
		Jitter:               0,
		MaxBackoff:           0,
	}, coordinatedShutdownCh, stats, logger)
}

func NewLSNTrackerWithConfigAndStats(retryConfig circuitbreaker.RetryConfig,
	coordinatedShutdownCh chan error,
	stats LSNTrackerStats,
	logger *zap.Logger) *PGBufferedLSNTracker {
	stopCtx, stop := context.WithCancel(context.Background())
	trackingQueue := writequeue.New[*trackedWriteRequest](logger)
	readyQueue := writequeue.New[*readyWriteEntry](logger)

	t := &PGBufferedLSNTracker{
		mu:                    sync.Mutex{},
		trackingQueue:         trackingQueue,
		readyQueue:            readyQueue,
		queueWrapper:          &readyWriteQueue{q: readyQueue},
		logger:                logger.Named("lsn-tracker"),
		retryConfig:           retryConfig,
		retryInterval:         retryWheelInterval(retryConfig),
		coordinatedShutdownCh: coordinatedShutdownCh,
		stopCtx:               stopCtx,
		stop:                  stop,
		stats:                 stats,
	}

	t.Start()

	return t
}

func retryWheelInterval(retryConfig circuitbreaker.RetryConfig) time.Duration {
	interval := defaultRetryWheelInterval
	if retryConfig.InitialBackoff > 0 && retryConfig.InitialBackoff < interval {
		interval = retryConfig.InitialBackoff
	}
	if interval < minRetryWheelInterval {
		return minRetryWheelInterval
	}

	return interval
}

type readyWriteQueue struct {
	q *writequeue.WriteQueue[*readyWriteEntry]
}

func (r *readyWriteQueue) Count() uint64 {
	return r.q.Count()
}

func (r *readyWriteQueue) Pop() (*pgwal.WriteRequest, bool) {
	for {
		entry, stopped := r.q.Pop()
		if stopped {
			return nil, true
		}

		if entry == nil || entry.trackedWrite == nil {
			continue
		}

		write := entry.trackedWrite.popQueuedWrite()
		if write == nil {
			continue
		}

		return write, false
	}
}

func (t *PGBufferedLSNTracker) Start() {
	t.startOnce.Do(func() {
		t.retryWheel, t.startErr = collection.NewTimingWheel(t.retryInterval, retryWheelSlots, t.onRetryTimer)
	})
	if t.startErr != nil {
		t.logger.Fatal("failed to start retry timing wheel", zap.Error(t.startErr))
	}
}

func (t *PGBufferedLSNTracker) onRetryTimer(_ any, value any) {
	entry, ok := value.(retryEntry)
	if !ok {
		t.logger.Error("unexpected retry timer value", zap.Any("value", value))

		return
	}

	t.stats.RetryQueueDec()

	if t.stopCtx.Err() != nil {
		return
	}

	t.logger.Info("retrying entry", zap.String(pglogger.LSNParam, entry.write.LSN.String()),
		zap.Uint32(pglogger.XIDParam, entry.write.XID),
		zap.Time("wait_until", entry.waitUntil))

	t.enqueueReadyWrite(entry.trackedWrite, entry.write)

	t.logger.Debug("retrying entry: end", zap.String(pglogger.LSNParam, entry.write.LSN.String()),
		zap.Uint32(pglogger.XIDParam, entry.write.XID),
		zap.Time("wait_until", entry.waitUntil),
	)
}

func (t *PGBufferedLSNTracker) scheduleRetry(entry retryEntry) {
	if t.stopCtx.Err() != nil {
		return
	}

	delay := roundRetryDelay(time.Until(entry.waitUntil), t.retryInterval)
	key := t.retrySeq.Add(1)
	if err := t.retryWheel.SetTimer(key, entry, delay); err != nil {
		if !errors.Is(err, collection.ErrClosed) {
			t.logger.Error("failed to schedule retry", zap.Error(err))
		}

		return
	}

	t.stats.RetryQueueInc()
}

func roundRetryDelay(delay, interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = minRetryWheelInterval
	}
	if delay <= 0 {
		return interval
	}
	if rem := delay % interval; rem > 0 {
		delay += interval - rem
	}

	return delay
}

func (t *PGBufferedLSNTracker) OnSuccess(resp *pgwal.Response) pglogrepl2json.CommitPoint {
	t.mu.Lock()
	defer t.mu.Unlock()

	acks := map[pglogrepl2json.CommitPoint][]uint32{}

	for _, entry := range resp.Entries {
		cp := pglogrepl2json.CommitPoint{
			LSN: entry.LSN,
			XID: entry.XID,
		}

		if debug := t.logger.Check(zap.DebugLevel, "entry acknowledged"); debug != nil {
			debug.Write(zap.String("lsn", entry.LSN.String()), zap.Uint32("xid", entry.XID), zap.Uint32("offset", entry.Offset))
		}

		offsets := acks[cp]
		offsets = append(offsets, entry.Offset)
		acks[cp] = offsets
	}

	if len(acks) > 0 {
		for cp, subOffsets := range acks {
			t.ack(cp, subOffsets)
		}
	}

	t.advanceMaxAcknowledged()

	return t.maxAcknowledged
}

// ack should be called within the mutex.
func (t *PGBufferedLSNTracker) ack(cp pglogrepl2json.CommitPoint, subOffsets []uint32) {
	entry, exists := t.findTrackedWrite(cp.LSN)
	if !exists {
		return
	}

	if fullAck := entry.ack(subOffsets); fullAck {
		t.writeQueueDec(entry.release())
	}
}

func (t *PGBufferedLSNTracker) writeQueueDec(count int) {
	for range count {
		t.stats.WriteQueueDec()
	}
}

func (t *PGBufferedLSNTracker) MaxAcknowledged() pglogrepl2json.CommitPoint {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.advanceMaxAcknowledged()

	return t.maxAcknowledged
}

func (t *PGBufferedLSNTracker) findTrackedWrite(lsn pglogrepl.LSN) (*trackedWriteRequest, bool) {
	return t.trackingQueue.Find(func(entry *trackedWriteRequest) bool {
		return entry.point.LSN == lsn
	})
}

func (t *PGBufferedLSNTracker) isAcknowledged(lsn pglogrepl.LSN) bool {
	return t.maxAcknowledged.LSN > 0 && lsn <= t.maxAcknowledged.LSN
}

func (t *PGBufferedLSNTracker) advanceMaxAcknowledged() {
	t.trackingQueue.PopUntil(func(entry *trackedWriteRequest) bool {
		if !entry.isAcknowledged() {
			return false
		}

		t.writeQueueDec(entry.release())
		t.maxAcknowledged = entry.point

		return true
	})
}

func (t *PGBufferedLSNTracker) pushTrackedWrite(entry *trackedWriteRequest) uint64 {
	return t.trackingQueue.PushBefore(entry, func(existing *trackedWriteRequest) bool {
		return existing.point.LSN > entry.point.LSN
	})
}

func (t *PGBufferedLSNTracker) enqueueReadyWrite(trackedWrite *trackedWriteRequest, write *pgwal.WriteRequest) uint64 {
	if !trackedWrite.queueWrite(write) {
		return t.readyQueue.Count()
	}

	t.stats.WriteQueueInc()

	return t.readyQueue.Push(&readyWriteEntry{
		trackedWrite: trackedWrite,
		write:        write,
	})
}

func (t *PGBufferedLSNTracker) debugXIDAndLSNIfEnabled(msg string, cp pglogrepl2json.CommitPoint) {
	if t.logger.Level() == zap.DebugLevel {
		var f []zap.Field
		f = append(f, zap.String(pglogger.LSNParam, cp.LSN.String()), zap.Uint32(pglogger.XIDParam, cp.XID))

		t.logger.Debug(msg, f...)
	}
}

type ackMap map[pglogrepl2json.CommitPoint][]uint32

const (
	maxRetriesErrMsg   = "skipped failed entry (max retries reached)"
	notRetryableErrMsg = "skipped failed entry (not retryable error)"
)

func getErrMsg(reason pgwal.WriterError) string {
	if !reason.IsRetryable() {
		return notRetryableErrMsg
	}

	return maxRetriesErrMsg
}

func (t *PGBufferedLSNTracker) processErrorResponse(errResp *pgwal.ErrResponse) (ackMap, []retryEntry, error) {
	// we don't care about errors when we're stopping
	if t.stopCtx.Err() != nil {
		return nil, nil, nil
	}

	var (
		retries []retryEntry
		m       = ackMap{}
	)

	addToAckMap := func(cp pglogrepl2json.CommitPoint, offset uint32) {
		if offsets, ok := m[cp]; ok {
			offsets = append(offsets, offset)
			m[cp] = offsets
		} else {
			m[cp] = []uint32{offset}
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, entry := range errResp.Entries {
		trackedWrite, exists := t.findTrackedWrite(entry.ResponseEntry.LSN)
		if !exists {
			if !t.isAcknowledged(entry.ResponseEntry.LSN) {
				t.logger.Fatal(errWriteIsMissing.Error(),
					zap.String(pglogger.LSNParam, entry.ResponseEntry.LSN.String()), zap.Uint32(pglogger.XIDParam, entry.ResponseEntry.XID),
					zap.String(pglogger.TableParam, entry.ResponseEntry.Table.String()), zap.String(pglogger.PKParam, entry.ResponseEntry.PK))
			}

			continue
		}

		if trackedWrite.isAcknowledged() {
			continue
		}

		failEntry := trackedWrite.getFailedEntry(entry.ResponseEntry.Offset, entry.Reason)

		if t.retryConfig.MaxConnectionRetries > 0 &&
			failEntry.downStreamDownRetries >= t.retryConfig.MaxConnectionRetries &&
			entry.Reason.IsDownStreamDown() {
			return nil, nil, fmt.Errorf("downstream is down: %w", entry.Reason)
		}

		t.logger.Debug("onerror: entry failed", zap.String("lsn", entry.ResponseEntry.LSN.String()),
			zap.Uint32("xid", entry.ResponseEntry.XID),
			zap.Uint32("offset", entry.ResponseEntry.Offset),
			zap.Int("retries", failEntry.totalRetries()),
		)

		if failEntry.retries >= t.retryConfig.MaxRetries || !entry.Reason.IsRetryable() {
			addToAckMap(pglogrepl2json.CommitPoint{
				LSN: entry.ResponseEntry.LSN,
				XID: entry.ResponseEntry.XID,
			}, entry.ResponseEntry.Offset)

			t.logger.Error(getErrMsg(entry.Reason),
				zap.String(pglogger.LSNParam, entry.ResponseEntry.LSN.String()), zap.Uint32(pglogger.XIDParam, entry.ResponseEntry.XID),
				zap.String(pglogger.TableParam, entry.ResponseEntry.Table.String()), zap.String(pglogger.PKParam, entry.ResponseEntry.PK),
				zap.Error(entry.Reason))

			continue
		}

		if t.retryConfig.MaxBackoff > 0 {
			re, err := t.buildRetryEntry(trackedWrite, entry, failEntry)
			if err != nil {
				return nil, nil, err
			}

			if re.write != nil {
				retries = append(retries, re)
			}
		}
	}

	return m, retries, nil
}

func (t *PGBufferedLSNTracker) buildRetryEntry(trackedWrite *trackedWriteRequest, entry *pgwal.ErrorResponseEntry, failEntry *failureEntry) (retryEntry, error) {
	newWrite := trackedWrite.retryWrite(entry.ResponseEntry.Offset)
	if newWrite != nil {
		backoff := t.backoff(failEntry)
		nextWrite := time.Now().Add(backoff)
		t.logger.Info("scheduled retry", zap.Duration("backoff", backoff), zap.Time("next_write", nextWrite), zap.String("lsn", entry.ResponseEntry.LSN.String()),
			zap.Uint32("xid", entry.ResponseEntry.XID), zap.Uint32("offset", entry.ResponseEntry.Offset), zap.Int("retries", failEntry.totalRetries()),
		)

		return retryEntry{
			trackedWrite: trackedWrite,
			write:        newWrite,
			waitUntil:    nextWrite,
		}, nil
	}

	return retryEntry{}, nil //nolint
}

func (t *PGBufferedLSNTracker) OnError(resp *pgwal.ErrResponse) {
	if resp == nil || len(resp.Entries) == 0 {
		return
	}

	acks, retries, err := t.processErrorResponse(resp)

	if err != nil {
		t.coordinatedShutdownCh <- err

		return
	}

	if len(acks) > 0 {
		t.mu.Lock()
		for cp, offsets := range acks {
			t.ack(cp, offsets)
		}
		t.mu.Unlock()
	}

	if len(retries) > 0 {
		for _, retry := range retries {
			t.scheduleRetry(retry)
		}
	}
}

func (t *PGBufferedLSNTracker) Stop() error {
	t.stopOnce.Do(func() {
		t.stop()
		t.trackingQueue.Stop()
		t.readyQueue.Stop()
		if t.retryWheel != nil {
			t.retryWheel.Stop()
		}
	})

	return nil
}

func (t *PGBufferedLSNTracker) Queue() writequeue.ReadonlyQueue[*pgwal.WriteRequest] {
	return t.queueWrapper
}

func (t *PGBufferedLSNTracker) WriteQueueSize() uint64 {
	return t.trackingQueue.Count()
}

func (t *PGBufferedLSNTracker) AddEmpty(lsn pglogrepl.LSN, xid uint32) {
	if t.stopCtx.Err() != nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isAcknowledged(lsn) {
		return
	}

	if trackedWrite, exists := t.findTrackedWrite(lsn); exists {
		if trackedWrite.ack([]uint32{0}) {
			t.writeQueueDec(trackedWrite.release())
		}
		return
	}

	trackedWrite := newTrackedWriteRequest(&pgwal.WriteRequest{
		LSN: lsn,
		XID: xid,
	})
	trackedWrite.clearQueued()
	_ = trackedWrite.ack([]uint32{0})
	t.writeQueueDec(trackedWrite.release())
	t.pushTrackedWrite(trackedWrite)
}

func (t *PGBufferedLSNTracker) Add(writeReq *pgwal.WriteRequest) uint64 {
	if t.stopCtx.Err() != nil {
		return 0
	}

	lsnStr := writeReq.LSN.String()

	t.logger.Debug("adding new entry: start", zap.String("lsn", lsnStr), zap.Uint32("xid", writeReq.XID))

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isAcknowledged(writeReq.LSN) {
		// do not send the write request if it has LSN less than already acknowledged LSN
		t.logger.Debug("adding new entry: already acknowledged", zap.String("lsn", lsnStr), zap.Uint32(pglogger.XIDParam, writeReq.XID))

		return t.readyQueue.Count()
	}

	if trackedWrite, exists := t.findTrackedWrite(writeReq.LSN); exists {
		if trackedWrite.isAcknowledged() {
			t.logger.Debug("adding new entry: already acknowledged", zap.String("lsn", lsnStr), zap.Uint32(pglogger.XIDParam, writeReq.XID))

			return t.readyQueue.Count()
		}

		// if offset already exists we need to check if it is failed or not
		if !trackedWrite.hasFailures() {
			// if request isn't failed - it can be inflight
			// we send it only when backoff is not configured
			t.logger.Debug("adding new entry: exists, not failed", zap.String("lsn", lsnStr),
				zap.Uint32("xid", writeReq.XID),
			)

			return t.readyQueue.Count()
		}

		t.logger.Debug("adding new entry: exists, retried", zap.Uint32("xid", writeReq.XID))
		if fullWrite := trackedWrite.fullWrite(); fullWrite != nil {
			// PG resends whole writes when the same LSN is observed after a failure.
			t.enqueueReadyWrite(trackedWrite, fullWrite)
		}

		return t.readyQueue.Count()
	}

	t.logger.Debug("adding new entry: sending write to the queue", zap.String("lsn", lsnStr), zap.Uint32("xid", writeReq.XID))

	trackedWrite := newTrackedWriteRequest(writeReq)
	t.pushTrackedWrite(trackedWrite)
	queueLen := t.enqueueReadyWrite(trackedWrite, writeReq)

	t.logger.Debug("adding new entry: end", zap.String("lsn", lsnStr), zap.Uint32("xid", writeReq.XID), zap.Uint64("queue_len", queueLen))

	return t.readyQueue.Count()
}

func (t *PGBufferedLSNTracker) backoff(entry *failureEntry) time.Duration {
	return t.retryConfig.Backoff(entry.retries)
}
