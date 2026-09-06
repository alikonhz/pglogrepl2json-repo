package offsettracker

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type ackUint64 struct {
	value uint64
}

func (a *ackUint64) Offset() uint64 {
	if a == nil {
		return 0
	}

	return a.value
}

func newAckUint64(val uint64) *ackUint64 {
	return &ackUint64{value: val}
}

func TestOffsetTrackerWithSubOffsets(t *testing.T) {
	tracker := NewOffsetTracker[*ackUint64]()

	tx1 := newAckUint64(30000)
	tx2 := newAckUint64(40000)
	tx3 := newAckUint64(50000)

	tracker.AddWithSubOffsets(tx1, []uint32{0, 1, 2})
	tracker.AddWithSubOffsets(tx2, []uint32{0, 1})
	tracker.AddWithSubOffsets(tx3, []uint32{0})

	// ack only last sub-offset of the 1st tx
	assert.False(t, tracker.AcknowledgeWithSubOffsets(tx1, []uint32{2}))

	// ack only first sub-offset of the 2nd tx
	assert.False(t, tracker.AcknowledgeWithSubOffsets(tx2, []uint32{1}))

	// ack full 3rd offset
	assert.True(t, tracker.AcknowledgeWithSubOffsets(tx3, []uint32{0}))

	maxAck := tracker.MaxAcknowledged()
	assert.Nil(t, maxAck)

	// ack all items from the 1st tx
	assert.True(t, tracker.AcknowledgeWithSubOffsets(tx1, []uint32{0, 1, 2}))

	// we acknowledged tx1/2 twice
	// make sure this doesn't cause issues by checking count of not acknowledged offsets for that tx
	// it should 0 and can't be less than zero
	assert.Equal(t, 0, tracker.entries[tx1.Offset()].subOffsets.Count())

	maxAck = tracker.MaxAcknowledged()
	assert.Equal(t, tx1.value, maxAck.Offset())

	// ack remaining item from the 2nd tx
	assert.True(t, tracker.AcknowledgeWithSubOffsets(tx2, []uint32{0}))

	maxAck = tracker.MaxAcknowledged()
	assert.Equal(t, tx3.value, maxAck.Offset())
}

func TestAcknowledgeShrinksArray(t *testing.T) {
	tracker := NewOffsetTracker[*ackUint64]()
	ln := cap(tracker.offsets)
	txs := make([]*ackUint64, ln)

	for i := 0; i < ln; i++ {
		tx := newAckUint64(uint64(i))
		txs[i] = tx
		tracker.AddWithSubOffsets(tx, []uint32{0, 1})
	}

	var (
		i      int
		lastTx *ackUint64
	)

	for i = 0; i < (ln/4)-1; i++ {
		tracker.AcknowledgeWithSubOffsets(txs[i], []uint32{0, 1})
		lastTx = txs[i]
	}

	maxAck := tracker.MaxAcknowledged()

	assert.Equal(t, lastTx.Offset(), maxAck.Offset())
	assert.Equal(t, i, tracker.ackCheckStart)
	assert.Equal(t, ln, len(tracker.offsets))

	lastTx = txs[i]

	tracker.AcknowledgeWithSubOffsets(lastTx, []uint32{0, 1})

	maxAck = tracker.MaxAcknowledged()

	assert.Equal(t, lastTx.Offset(), maxAck.Offset())
	assert.Equal(t, ln-ln/4, len(tracker.offsets)) // offset array should be shrunk
	assert.Equal(t, 0, tracker.ackCheckStart)      // ackCheckStart should be reset to 0

	i++

	lastTx = txs[i]

	tracker.AcknowledgeWithSubOffsets(lastTx, []uint32{0, 1})

	maxAck = tracker.MaxAcknowledged()

	assert.Equal(t, lastTx.Offset(), maxAck.Offset())
	assert.Equal(t, 1, tracker.ackCheckStart)
}
