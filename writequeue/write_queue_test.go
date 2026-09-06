package writequeue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type data struct {
	name string
	size uint32
}

func (d *data) Size() uint32 {
	return d.size
}

func TestExistingElementPopped(t *testing.T) {
	q := New[*data](zap.NewNop())

	q.Push(&data{name: "test"})

	d, _ := q.Pop()

	if d.name != "test" {
		t.Error("element not found")
	}
}

func TestAllElementsPopped(t *testing.T) {
	q := New[*data](zap.NewNop())

	for i := 0; i < 10; i++ {
		q.Push(&data{name: fmt.Sprintf("test%d", i)})
	}

	for i := 0; i < 10; i++ {
		d, _ := q.Pop()
		assert.Equal(t, fmt.Sprintf("test%d", i), d.name)
	}
}

func TestNewElementPopped(t *testing.T) {
	q := New[*data](zap.NewNop())

	var (
		resCh   = make(chan *data)
		wg      = sync.WaitGroup{}
		timeout = 10 * time.Millisecond
	)

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	wg.Add(1)

	go func() {
		wg.Done()
		res, _ := q.Pop()
		resCh <- res
	}()

	go func() {
		wg.Wait()
		q.Push(&data{name: "test"})
	}()

	var d *data
	select {
	case d = <-resCh:
		break
	case <-ctx.Done():
		t.Error("timeout")
	}

	require.NotNil(t, d)
	assert.Equal(t, "test", d.name)
}

func TestPutAtTail(t *testing.T) {
	q := New[*data](zap.NewNop())

	q.Push(&data{name: "test1"})
	q.PutAtTail(&data{name: "test2"})

	// first should be test2
	el1, _ := q.Pop()
	assert.Equal(t, "test2", el1.name)

	el2, _ := q.Pop()
	assert.Equal(t, "test1", el2.name)

	assert.Nil(t, q.tail)
	assert.Nil(t, q.head)
}

func TestPushBeforeInsertsBeforeFirstMatchingItem(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 3})
	q.PushBefore(&intData{data: 2}, func(entry *intData) bool {
		return entry.data > 2
	})

	assertIntQueueLinks(t, q, 1, 2, 3)

	for _, expected := range []int{1, 2, 3} {
		entry, stopped := q.Pop()
		require.False(t, stopped)
		require.Equal(t, expected, entry.data)
	}
}

func TestPushBeforeInsertsBeforeTail(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})
	q.PushBefore(&intData{data: 0}, func(entry *intData) bool {
		return entry.data >= 1
	})

	assertIntQueueLinks(t, q, 0, 1, 2)
}

func TestPushBeforeAppendsWhenNoItemMatches(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})
	q.PushBefore(&intData{data: 3}, func(entry *intData) bool {
		return entry.data > 3
	})

	assertIntQueueLinks(t, q, 1, 2, 3)
}

func TestPushBeforeInsertsNearHead(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})
	q.Push(&intData{data: 4})
	q.PushBefore(&intData{data: 3}, func(entry *intData) bool {
		return entry.data > 3
	})

	assertIntQueueLinks(t, q, 1, 2, 3, 4)
}

func TestPopUntilRemovesMatchingItemsFromTail(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})
	q.Push(&intData{data: 3})
	q.Push(&intData{data: 4})

	q.PopUntil(func(entry *intData) bool {
		return entry.data < 3
	})

	assertIntQueueLinks(t, q, 3, 4)
	assert.Equal(t, uint64(2), q.Count())

	first, stopped := q.Pop()
	require.False(t, stopped)
	require.Equal(t, 3, first.data)

	second, stopped := q.Pop()
	require.False(t, stopped)
	require.Equal(t, 4, second.data)

	assert.Equal(t, uint64(0), q.Count())
	assert.Nil(t, q.tail)
	assert.Nil(t, q.head)
}

func TestPopUntilClearsNewTailPreviousLink(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})
	q.Push(&intData{data: 3})
	q.PopUntil(func(entry *intData) bool {
		return entry.data < 2
	})

	assertIntQueueLinks(t, q, 2, 3)

	q.PushBefore(&intData{data: 1}, func(entry *intData) bool {
		return entry.data >= 2
	})

	assertIntQueueLinks(t, q, 1, 2, 3)
}

func TestPopUntilStopsAtFirstNonMatchingItem(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})

	q.PopUntil(func(entry *intData) bool {
		return entry.data > 10
	})

	assert.Equal(t, uint64(2), q.Count())

	first, stopped := q.Pop()
	require.False(t, stopped)
	require.Equal(t, 1, first.data)
}

func TestPopUntilClearsQueueWhenAllItemsMatch(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})

	q.PopUntil(func(entry *intData) bool {
		return entry.data <= 2
	})

	assert.Equal(t, uint64(0), q.Count())
	assert.Nil(t, q.tail)
	assert.Nil(t, q.head)
}

type intData struct {
	data int
}

func (i *intData) Size() uint32 {
	return 1
}

func TestPopClearsNewTailPreviousLink(t *testing.T) {
	q := New[*intData](zap.NewNop())

	q.Push(&intData{data: 1})
	q.Push(&intData{data: 2})
	q.Push(&intData{data: 3})

	entry, stopped := q.Pop()
	require.False(t, stopped)
	require.Equal(t, 1, entry.data)

	assertIntQueueLinks(t, q, 2, 3)

	q.PushBefore(&intData{data: 1}, func(entry *intData) bool {
		return entry.data >= 2
	})

	assertIntQueueLinks(t, q, 1, 2, 3)
}

func assertIntQueueLinks(t *testing.T, q *WriteQueue[*intData], expected ...int) {
	t.Helper()

	var prev *writeQueueNode[*intData]
	node := q.tail
	for _, value := range expected {
		require.NotNil(t, node)
		require.True(t, node.prev == prev)
		require.Equal(t, value, node.data.data)

		prev = node
		node = node.next
	}

	require.Nil(t, node)
	if len(expected) == 0 {
		require.Nil(t, q.tail)
		require.Nil(t, q.head)
		return
	}

	require.True(t, q.head == prev)
	require.Nil(t, q.head.next)

	var next *writeQueueNode[*intData]
	node = q.head
	for i := len(expected) - 1; i >= 0; i-- {
		require.NotNil(t, node)
		require.True(t, node.next == next)
		require.Equal(t, expected[i], node.data.data)

		next = node
		node = node.prev
	}

	require.Nil(t, node)
	require.True(t, q.tail == next)
}

func TestLoop(t *testing.T) {
	const queueSize = 10

	q := New[*intData](zap.NewNop())

	resCh := make(chan *intData, queueSize)

	returned1 := false
	returned2 := false

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for {
			res, stopped := q.Pop()

			if stopped {
				returned1 = true

				wg.Done()

				return
			}

			resCh <- res
		}
	}()

	var actual []int

	go func() {
		for i := range resCh {
			actual = append(actual, i.data)

			if len(actual) == queueSize {
				q.Stop()
				wg.Done()

				returned2 = true

				return
			}
		}
	}()

	var expected []int

	for i := 100; i < 100+queueSize; i++ {
		q.Push(&intData{data: i})
		expected = append(expected, i)
	}

	wg.Wait()

	testRes, stopped := q.Pop()

	assert.True(t, returned1)
	assert.True(t, returned2)
	assert.True(t, stopped)
	assert.Zero(t, testRes)
	assert.Equal(t, expected, actual)
}
