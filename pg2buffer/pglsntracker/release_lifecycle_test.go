package pglsntracker

import (
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type releaseTrackingTuple struct {
	releases atomic.Int32
}

func (t *releaseTrackingTuple) Get(string) (any, bool) {
	return nil, false
}

func (t *releaseTrackingTuple) GetValue(string) any {
	return nil
}

func (t *releaseTrackingTuple) Set(string, any) {
}

func (t *releaseTrackingTuple) Keys() []string {
	return nil
}

func (t *releaseTrackingTuple) Iter() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {}
}

func (t *releaseTrackingTuple) MarshalJSON() ([]byte, error) {
	return []byte("{}"), nil
}

func (t *releaseTrackingTuple) CustomMarshalJSON([]string, []orderedmap.ValuePair) ([]byte, error) {
	return []byte("{}"), nil
}

func (t *releaseTrackingTuple) Size() uint32 {
	return 0
}

func (t *releaseTrackingTuple) Release() {
	t.releases.Add(1)
}

func (t *releaseTrackingTuple) releaseCount() int32 {
	return t.releases.Load()
}

func TestLSNTrackerReleasesEntriesOnlyAfterFullSuccessAck(t *testing.T) {
	tracker := newReleaseLifecycleTracker(t, circuitbreaker.RetryConfig{
		MaxRetries:           3,
		MaxConnectionRetries: 3,
	})

	firstTuple := &releaseTrackingTuple{}
	firstPrevTuple := &releaseTrackingTuple{}
	secondTuple := &releaseTrackingTuple{}
	write := releaseLifecycleWrite(1,
		&pgwal.WriteEntry{Tuple: firstTuple, PrevTuple: firstPrevTuple, Offset: 0},
		&pgwal.WriteEntry{Tuple: secondTuple, Offset: 1},
	)

	tracker.Add(write)

	tracker.OnSuccess(releaseLifecycleSuccess(write, 0))
	require.Equal(t, int32(0), firstTuple.releaseCount())
	require.Equal(t, int32(0), firstPrevTuple.releaseCount())
	require.Equal(t, int32(0), secondTuple.releaseCount())

	tracker.OnSuccess(releaseLifecycleSuccess(write, 1))
	require.Equal(t, int32(1), firstTuple.releaseCount())
	require.Equal(t, int32(1), firstPrevTuple.releaseCount())
	require.Equal(t, int32(1), secondTuple.releaseCount())

	tracker.OnSuccess(releaseLifecycleSuccess(write, 0, 1))
	require.Equal(t, int32(1), firstTuple.releaseCount())
	require.Equal(t, int32(1), firstPrevTuple.releaseCount())
	require.Equal(t, int32(1), secondTuple.releaseCount())
}

func TestLSNTrackerDoesNotReleaseRetryableErrorBeforeSuccess(t *testing.T) {
	tracker := newReleaseLifecycleTracker(t, circuitbreaker.RetryConfig{
		MaxRetries:           3,
		MaxConnectionRetries: 3,
		InitialBackoff:       time.Nanosecond,
		Multiplier:           1,
		Jitter:               0,
		MaxBackoff:           time.Nanosecond,
	})

	tuple := &releaseTrackingTuple{}
	write := releaseLifecycleWrite(2, &pgwal.WriteEntry{Tuple: tuple, Offset: 0})

	tracker.Add(write)
	tracker.OnError(releaseLifecycleError(write, true, 0))
	require.Equal(t, int32(0), tuple.releaseCount())

	tracker.OnSuccess(releaseLifecycleSuccess(write, 0))
	require.Equal(t, int32(1), tuple.releaseCount())
}

func TestLSNTrackerReleasesSkippedFailedEntriesAfterFullAck(t *testing.T) {
	tracker := newReleaseLifecycleTracker(t, circuitbreaker.RetryConfig{
		MaxRetries:           3,
		MaxConnectionRetries: 3,
	})

	firstTuple := &releaseTrackingTuple{}
	secondTuple := &releaseTrackingTuple{}
	write := releaseLifecycleWrite(3,
		&pgwal.WriteEntry{Tuple: firstTuple, Offset: 0},
		&pgwal.WriteEntry{Tuple: secondTuple, Offset: 1},
	)

	tracker.Add(write)

	tracker.OnError(releaseLifecycleError(write, false, 0))
	require.Equal(t, int32(0), firstTuple.releaseCount())
	require.Equal(t, int32(0), secondTuple.releaseCount())

	tracker.OnSuccess(releaseLifecycleSuccess(write, 1))
	require.Equal(t, int32(1), firstTuple.releaseCount())
	require.Equal(t, int32(1), secondTuple.releaseCount())
}

func newReleaseLifecycleTracker(t *testing.T, retryConfig circuitbreaker.RetryConfig) *PGBufferedLSNTracker {
	t.Helper()

	tracker := NewLSNTrackerWithConfigAndStats(makeReleaseLifecycleRetryConfig(retryConfig), make(chan error, 1), &noStats{}, zap.NewNop())
	t.Cleanup(func() {
		require.NoError(t, tracker.Stop())
	})

	return tracker
}

func makeReleaseLifecycleRetryConfig(retryConfig circuitbreaker.RetryConfig) circuitbreaker.RetryConfig {
	if retryConfig.MaxRetries == 0 {
		retryConfig.MaxRetries = 3
	}
	if retryConfig.MaxConnectionRetries == 0 {
		retryConfig.MaxConnectionRetries = 3
	}

	return retryConfig
}

func releaseLifecycleWrite(lsn pglogrepl.LSN, entries ...*pgwal.WriteEntry) *pgwal.WriteRequest {
	table := pgschema.MakeTableName("public", "release_lifecycle")
	for i, entry := range entries {
		entry.PK = string(rune('a' + i))
		entry.Table = table
	}

	return &pgwal.WriteRequest{
		Entries: entries,
		LSN:     lsn,
		XID:     uint32(lsn),
	}
}

func releaseLifecycleSuccess(write *pgwal.WriteRequest, offsets ...uint32) *pgwal.Response {
	entries := make([]*pgwal.ResponseEntry, 0, len(offsets))
	for _, offset := range offsets {
		entry := write.GetEntryByOffset(offset)
		entries = append(entries, &pgwal.ResponseEntry{
			PK:     entry.PK,
			Table:  entry.Table,
			LSN:    write.LSN,
			XID:    write.XID,
			Offset: offset,
		})
	}

	return &pgwal.Response{Entries: entries}
}

func releaseLifecycleError(write *pgwal.WriteRequest, retryable bool, offsets ...uint32) *pgwal.ErrResponse {
	entries := make([]*pgwal.ErrorResponseEntry, 0, len(offsets))
	for _, offset := range offsets {
		entry := write.GetEntryByOffset(offset)
		entries = append(entries, &pgwal.ErrorResponseEntry{
			ResponseEntry: pgwal.ResponseEntry{
				PK:     entry.PK,
				Table:  entry.Table,
				LSN:    write.LSN,
				XID:    write.XID,
				Offset: offset,
			},
			Reason: writerError{
				err:         "release lifecycle error",
				isRetryable: retryable,
			},
		})
	}

	return &pgwal.ErrResponse{Entries: entries}
}
