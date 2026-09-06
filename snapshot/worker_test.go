package snapshot

import (
	"slices"
	"sync"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
)

func TestSnapshotLSNTracker_NextLSNSequential(t *testing.T) {
	tracker := &snapshotLSNTracker{}

	assert.Equal(t, pglogrepl.LSN(1), tracker.NextLSN())
	assert.Equal(t, pglogrepl.LSN(2), tracker.NextLSN())
	assert.Equal(t, pglogrepl.LSN(3), tracker.NextLSN())
}

func TestSnapshotLSNTracker_MarkSubmittedKeepsMaximum(t *testing.T) {
	tracker := &snapshotLSNTracker{}

	tracker.MarkSubmitted(pglogrepl.LSN(5))
	tracker.MarkSubmitted(pglogrepl.LSN(3))
	tracker.MarkSubmitted(pglogrepl.LSN(9))
	tracker.MarkSubmitted(pglogrepl.LSN(7))

	assert.Equal(t, pglogrepl.LSN(9), tracker.MaxSubmitted())
}

func TestSnapshotLSNTracker_NextLSNConcurrent(t *testing.T) {
	tracker := &snapshotLSNTracker{}

	const total = 64

	values := make([]int, total)
	var wg sync.WaitGroup

	for i := range total {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			values[idx] = int(tracker.NextLSN())
		}(i)
	}

	wg.Wait()

	slices.Sort(values)

	for i := range total {
		assert.Equal(t, i+1, values[i])
	}
}
