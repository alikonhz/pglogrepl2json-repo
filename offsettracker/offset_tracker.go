package offsettracker

import (
	"slices"
	"sync"

	"github.com/kelindar/bitmap"
)

type Offset interface {
	Offset() uint64
}

type offsetEntry[T Offset] struct {
	entry T

	subOffsets bitmap.Bitmap
}

func newOffsetEntry[T Offset](offset T, subOffsets []uint32) *offsetEntry[T] {
	m := bitmap.Bitmap{}

	for _, subOffset := range subOffsets {
		// we're setting the bit meaning that the subOffset is to be acknowledged
		m.Set(subOffset)
	}

	return &offsetEntry[T]{
		entry:      offset,
		subOffsets: m,
	}
}

type OffsetTracker[T Offset] struct {
	mu            sync.Mutex
	acked         map[uint64]bool // Tracks acknowledgment status of offsets
	entries       map[uint64]*offsetEntry[T]
	offsets       []uint64
	lastMaxAck    T
	ackCheckStart int

	defaultT T
}

// NewOffsetTracker initializes the tracker.
func NewOffsetTracker[T Offset]() *OffsetTracker[T] {
	const defaultMaxOffset = 1000

	var defaultT T

	return &OffsetTracker[T]{
		mu:            sync.Mutex{},
		acked:         make(map[uint64]bool),
		entries:       make(map[uint64]*offsetEntry[T]),
		offsets:       make([]uint64, 0, defaultMaxOffset),
		ackCheckStart: 0,
		defaultT:      defaultT,
	}
}

var (
	singleElement = []uint32{0}
)

// Add adds an offset for tracking if it doesn't exist.
func (t *OffsetTracker[T]) Add(offset T) ProbeResult {
	return t.AddWithSubOffsets(offset, singleElement)
}

func (t *OffsetTracker[T]) Probe(off uint64) ProbeResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	var (
		exists bool
		acked  bool
	)

	if _, exists = t.acked[off]; exists {
		acked = t.acked[off]
	}

	return ProbeResult{
		Exists:       exists,
		Acknowledged: acked,
	}
}

// AddWithSubOffsets adds an offset with sub-offsets for tracking if it doesn't exist.
func (t *OffsetTracker[T]) AddWithSubOffsets(offset T, subOffsets []uint32) ProbeResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	off := offset.Offset()

	if off < t.lastMaxAck.Offset() {
		return ProbeResult{
			Exists:       false,
			Acknowledged: true,
		}
	}

	if _, exists := t.acked[off]; exists {
		acked := t.acked[off]

		return ProbeResult{
			Exists:       true,
			Acknowledged: acked,
		}
	}

	t.acked[off] = false
	t.offsets = append(t.offsets, off)
	t.entries[off] = newOffsetEntry[T](offset, subOffsets)
	slices.Sort(t.offsets)

	return ProbeResult{
		Exists:       false,
		Acknowledged: false,
	}
}

func (t *OffsetTracker[T]) AcknowledgeWithSubOffsets(offset T, subOffsets []uint32) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	off := offset.Offset()
	if acked := t.acked[off]; acked {
		// already acked
		return true
	}

	entry, entryExists := t.entries[off]
	if !entryExists {
		return false
	}

	for _, subOffset := range subOffsets {
		entry.subOffsets.Remove(subOffset)
	}

	var fullyAcked bool

	if entry.subOffsets.Count() == 0 {
		t.acked[off] = true
		fullyAcked = true
	}

	return fullyAcked
}

type ProbeResult struct {
	Exists       bool
	Acknowledged bool
}

// MaxAcknowledged finds the maximum contiguous acknowledged offset.
func (t *OffsetTracker[T]) MaxAcknowledged() T {
	t.mu.Lock()
	defer t.mu.Unlock()

	var (
		maxAcknowledged  uint64
		maxAcknowledgedT *offsetEntry[T]
		i                = t.ackCheckStart
	)

	const maxGarbageFactor = 4

	// i.e. cleanup when the number of acked entries (garbage) reaches 25%
	delThreshold := cap(t.offsets) / maxGarbageFactor

	for ; i < len(t.offsets); i++ {
		if !t.acked[t.offsets[i]] {
			break
		}

		maxAcknowledged = t.offsets[i]
		maxAcknowledgedT = t.entries[maxAcknowledged]
	}

	// not found
	if maxAcknowledged == 0 || maxAcknowledgedT == nil {
		return t.lastMaxAck
	}

	t.ackCheckStart = i

	if i >= delThreshold {
		for j := 0; j < i; j++ {
			offset := t.offsets[j]

			delete(t.acked, offset)
			delete(t.entries, offset)
		}

		t.offsets = t.offsets[i:]
		t.ackCheckStart = 0
	}

	t.lastMaxAck = maxAcknowledgedT.entry

	return maxAcknowledgedT.entry
}
