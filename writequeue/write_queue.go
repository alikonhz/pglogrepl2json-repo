package writequeue

import (
	"sync"

	"go.uber.org/zap"
)

type ReadonlyQueue[T QueueEntry] interface {
	Count() uint64
	Pop() (T, bool)
}

type QueueEntry interface {
	Size() uint32
}

type WriteQueue[T QueueEntry] struct {
	mu    sync.Mutex
	tail  *writeQueueNode[T]
	head  *writeQueueNode[T]
	count uint64

	cond    *sync.Cond
	stopped bool
	logger  *zap.Logger
}

type writeQueueNode[T any] struct {
	data T
	next *writeQueueNode[T]
	prev *writeQueueNode[T]
}

func New[T QueueEntry](logger *zap.Logger) *WriteQueue[T] {
	wq := &WriteQueue[T]{
		mu:      sync.Mutex{},
		count:   0,
		tail:    nil,
		head:    nil,
		cond:    nil,
		stopped: false,
		logger:  logger,
	}

	wq.cond = sync.NewCond(&wq.mu)

	return wq
}

func (q *WriteQueue[T]) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.stopped = true

	q.cond.Signal()
}

func (q *WriteQueue[T]) PutAtTail(item T) uint64 {
	node := newNode(item)

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail == nil {
		q.tail = node
		q.head = node
	} else {
		node.next = q.tail
		q.tail.prev = node
		q.tail = node
	}

	q.count += uint64(item.Size())

	q.cond.Signal()

	return q.count
}

func (q *WriteQueue[T]) Push(item T) uint64 {
	node := newNode(item)

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail == nil {
		q.tail = node
		q.head = node
	} else {
		q.head.next = node
		node.prev = q.head
		q.head = node
	}

	q.count += uint64(item.Size())

	q.cond.Signal()

	return q.count
}

func (q *WriteQueue[T]) PushBefore(item T, before UntilFunc[T]) uint64 {
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
	} else if !before(q.head.data) {
		q.insertAfterNode(node, q.head)
	} else {
		fromTail := q.tail
		fromHead := q.head

		for {
			nextFromTail := fromTail.next
			if nextFromTail == nil || before(nextFromTail.data) {
				q.insertAfterNode(node, fromTail)
				break
			}

			prevFromHead := fromHead.prev
			if prevFromHead == nil {
				q.insertBeforeNode(node, fromHead)
				break
			}
			if !before(prevFromHead.data) {
				q.insertAfterNode(node, prevFromHead)
				break
			}

			fromTail = nextFromTail
			fromHead = prevFromHead
		}
	}

	q.count += uint64(item.Size())

	q.cond.Signal()

	return q.count
}

func (q *WriteQueue[T]) insertBeforeNode(node, target *writeQueueNode[T]) {
	node.next = target
	node.prev = target.prev

	if target.prev == nil {
		q.tail = node
	} else {
		target.prev.next = node
	}

	target.prev = node
}

func (q *WriteQueue[T]) insertAfterNode(node, target *writeQueueNode[T]) {
	node.next = target.next
	node.prev = target

	if target.next == nil {
		q.head = node
	} else {
		target.next.prev = node
	}

	target.next = node
}

func (q *WriteQueue[T]) Count() uint64 {
	q.mu.Lock()
	count := q.count
	q.mu.Unlock()

	return count
}

type UntilFunc[T QueueEntry] func(entry T) bool

func (q *WriteQueue[T]) Find(f UntilFunc[T]) (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var zero T

	if f == nil || q.stopped {
		return zero, false
	}

	traversed := 0
	for node := q.tail; node != nil; node = node.next {
		traversed++
		if f(node.data) {
			return node.data, true
		}
	}

	return zero, false
}

func (q *WriteQueue[T]) WaitUntil(f UntilFunc[T]) (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var zero T

	for {
		if q.stopped {
			return zero, true
		}

		if f != nil {
			for node := q.tail; node != nil; node = node.next {
				if f(node.data) {
					return node.data, false
				}
			}
		}

		q.cond.Wait()
	}
}

func (q *WriteQueue[T]) Sum(f func(entry T) uint64) uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	if f == nil || q.stopped {
		return 0
	}

	var total uint64
	for node := q.tail; node != nil; node = node.next {
		total += f(node.data)
	}

	return total
}

func (q *WriteQueue[T]) Signal() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.cond.Signal()
}

func (q *WriteQueue[T]) PopUntil(f UntilFunc[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if f == nil || q.stopped || q.tail == nil {
		return
	}

	for q.tail != nil {
		if !f(q.tail.data) {
			break
		}

		q.count -= uint64(q.tail.data.Size())
		q.tail = q.tail.next
		if q.tail != nil {
			q.tail.prev = nil
		}
	}

	if q.tail == nil {
		q.count = 0
		q.head = nil
	}
}

func (q *WriteQueue[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var zero T

	if q.stopped {
		return zero, true
	}

	for q.tail == nil && !q.stopped {
		q.cond.Wait()
	}

	if q.stopped {
		return zero, true
	}

	result := q.tail
	q.tail = q.tail.next
	q.count -= uint64(result.data.Size())

	if q.tail == nil {
		q.count = 0
		q.head = nil
	} else {
		q.tail.prev = nil
	}

	return result.data, q.stopped
}

func newNode[T any](data T) *writeQueueNode[T] {
	return &writeQueueNode[T]{
		data: data,
	}
}
