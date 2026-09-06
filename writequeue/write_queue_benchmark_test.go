package writequeue

import (
	"fmt"
	"testing"

	"go.uber.org/zap"
)

func BenchmarkPushBefore(b *testing.B) {
	approaches := []struct {
		name string
		push func(*WriteQueue[*intData], *intData, UntilFunc[*intData]) uint64
	}{
		{
			name: "bidirectional",
			push: func(q *WriteQueue[*intData], item *intData, before UntilFunc[*intData]) uint64 {
				return q.PushBefore(item, before)
			},
		},
		{
			name: "linear",
			push: func(q *WriteQueue[*intData], item *intData, before UntilFunc[*intData]) uint64 {
				return pushBeforeLinear(q, item, before)
			},
		},
	}

	for _, approach := range approaches {
		b.Run(approach.name, func(b *testing.B) {
			for _, queueSize := range []int{1000, 10000, 100000} {
				b.Run(fmt.Sprintf("elements_%d_append", queueSize), func(b *testing.B) {
					benchmarkPushBeforeAppend(b, queueSize, approach.push)
				})

				b.Run(fmt.Sprintf("elements_%d_first", queueSize), func(b *testing.B) {
					benchmarkPushBeforeFirst(b, queueSize, approach.push)
				})

				b.Run(fmt.Sprintf("elements_%d_middle", queueSize), func(b *testing.B) {
					benchmarkPushBeforeMiddle(b, queueSize, approach.push)
				})

				b.Run(fmt.Sprintf("elements_%d_near_head", queueSize), func(b *testing.B) {
					benchmarkPushBeforeNearHead(b, queueSize, approach.push)
				})
			}
		})
	}
}

func benchmarkPushBeforeAppend(
	b *testing.B,
	queueSize int,
	push func(*WriteQueue[*intData], *intData, UntilFunc[*intData]) uint64,
) {
	q := newIntDataQueue(queueSize)
	originalHead := q.head
	item := &intData{data: queueSize}
	before := func(entry *intData) bool {
		return entry.data > queueSize
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		push(q, item, before)
		b.StopTimer()

		// Restore the queue to the requested size without timing cleanup.
		originalHead.next = nil
		q.head = originalHead
		q.count = uint64(queueSize)
	}
}

func benchmarkPushBeforeFirst(
	b *testing.B,
	queueSize int,
	push func(*WriteQueue[*intData], *intData, UntilFunc[*intData]) uint64,
) {
	q := newIntDataQueue(queueSize)
	originalTail := q.tail
	item := &intData{data: -1}
	before := func(entry *intData) bool {
		return entry.data >= 0
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		push(q, item, before)
		b.StopTimer()

		// Restore the queue to the requested size without timing cleanup.
		q.tail = originalTail
		originalTail.prev = nil
		q.count = uint64(queueSize)
	}
}

func benchmarkPushBeforeMiddle(
	b *testing.B,
	queueSize int,
	push func(*WriteQueue[*intData], *intData, UntilFunc[*intData]) uint64,
) {
	q := newIntDataQueue(queueSize)
	middleValue := queueSize / 2
	prevMiddle, middle := findIntDataInsertNeighbors(q, middleValue)

	item := &intData{data: middleValue}
	before := func(entry *intData) bool {
		return entry.data >= middleValue
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		push(q, item, before)
		b.StopTimer()

		// Restore the queue to the requested size without timing cleanup.
		prevMiddle.next = middle
		middle.prev = prevMiddle
		q.count = uint64(queueSize)
	}
}

func benchmarkPushBeforeNearHead(
	b *testing.B,
	queueSize int,
	push func(*WriteQueue[*intData], *intData, UntilFunc[*intData]) uint64,
) {
	q := newIntDataQueue(queueSize)
	insertValue := queueSize - 1
	prevHead := q.head.prev
	originalHead := q.head

	item := &intData{data: insertValue}
	before := func(entry *intData) bool {
		return entry.data >= insertValue
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		push(q, item, before)
		b.StopTimer()

		// Restore the queue to the requested size without timing cleanup.
		prevHead.next = originalHead
		originalHead.prev = prevHead
		q.count = uint64(queueSize)
	}
}

func newIntDataQueue(queueSize int) *WriteQueue[*intData] {
	q := New[*intData](zap.NewNop())
	for i := 0; i < queueSize; i++ {
		q.Push(&intData{data: i})
	}

	return q
}

func findIntDataInsertNeighbors(
	q *WriteQueue[*intData],
	insertValue int,
) (*writeQueueNode[*intData], *writeQueueNode[*intData]) {
	prev := q.tail
	for prev.next != nil && prev.next.data.data < insertValue {
		prev = prev.next
	}

	return prev, prev.next
}

func pushBeforeLinear[T QueueEntry](q *WriteQueue[T], item T, before UntilFunc[T]) uint64 {
	if before == nil {
		return q.Push(item)
	}

	node := newNode(item)

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail == nil {
		q.tail = node
		q.head = node
	} else if before(q.tail.data) {
		q.insertBeforeNode(node, q.tail)
	} else {
		cur := q.tail
		for cur.next != nil && !before(cur.next.data) {
			cur = cur.next
		}

		q.insertAfterNode(node, cur)
	}

	q.count += uint64(item.Size())

	q.cond.Signal()

	return q.count
}
