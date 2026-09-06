package entrypool

import (
	"fmt"
	"sync"
	"testing"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/stretchr/testify/require"
)

func TestEntryTuplePoolReturnsClearedTuple(t *testing.T) {
	pool := NewPool()
	km := keymap.New("id", "name")

	tuple := pool.Get("public.users", "1", km)
	tuple.Set("id", 1)
	tuple.Set("name", "Alice")
	tuple.Release()

	reused := pool.Get("public.users", "1", km)
	defer reused.Release()

	require.Equal(t, uint32(0), reused.Size())

	_, ok := reused.Get("id")
	require.False(t, ok)

	j, err := reused.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `{}`, string(j))
}

func TestEntryTuplePoolCreatesOnePoolPerKeyUnderConcurrency(t *testing.T) {
	pool := NewPool()
	km := keymap.New("id", "name")

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tuple := pool.Get("public.users", "1", km)
				tuple.Set("id", id)
				tuple.Set("name", "Alice")
				tuple.Release()
			}
		}(i)
	}
	wg.Wait()

	var poolCount int
	pool.pools.Range(func(_, _ any) bool {
		poolCount++
		return true
	})

	require.Equal(t, 1, poolCount)
}

func TestEntryTuplePoolSeparatesTableAndVersionKeyMaps(t *testing.T) {
	pool := NewPool()

	usersV1KeyMap := keymap.New("id", "name")
	usersV2KeyMap := keymap.New("id", "email")
	ordersV1KeyMap := keymap.New("id", "total")

	usersV1 := pool.Get("public.users", "1", usersV1KeyMap)
	usersV1Map := usersV1.(*pooledOrderedMap).om
	usersV1.Set("name", "Alice")
	usersV1.Release()

	usersV2 := pool.Get("public.users", "2", usersV2KeyMap)
	usersV2Map := usersV2.(*pooledOrderedMap).om
	require.NotSame(t, usersV1Map, usersV2Map)
	require.NotPanics(t, func() {
		usersV2.Set("email", "alice@example.com")
	})
	usersV2.Release()

	ordersV1 := pool.Get("public.orders", "1", ordersV1KeyMap)
	ordersV1Map := ordersV1.(*pooledOrderedMap).om
	require.NotSame(t, usersV1Map, ordersV1Map)
	require.NotPanics(t, func() {
		ordersV1.Set("total", 42)
	})
	ordersV1.Release()

	reusedUsersV1 := pool.Get("public.users", "1", usersV1KeyMap)
	defer reusedUsersV1.Release()
	require.NotPanics(t, func() {
		reusedUsersV1.Set("name", "Bob")
	})
}

var benchmarkTupleSize uint32

func BenchmarkOrderedMapNewSet(b *testing.B) {
	for _, columnCount := range []int{5, 20, 100} {
		b.Run(fmt.Sprintf("columns_%d", columnCount), func(b *testing.B) {
			km, columns, values := benchmarkTupleData(columnCount)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tuple := orderedmap.New(km)
				for colIndex, column := range columns {
					tuple.Set(column, values[colIndex])
				}
				benchmarkTupleSize = tuple.Size()
			}
		})
	}
}

func BenchmarkEntryTuplePoolGetSetRelease(b *testing.B) {
	for _, columnCount := range []int{5, 20, 100} {
		b.Run(fmt.Sprintf("columns_%d", columnCount), func(b *testing.B) {
			km, columns, values := benchmarkTupleData(columnCount)
			pool := NewPool()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tuple := pool.Get("public.users", "1", km)
				for colIndex, column := range columns {
					tuple.Set(column, values[colIndex])
				}
				benchmarkTupleSize = tuple.Size()
				tuple.Release()
			}
		})
	}
}

func benchmarkTupleData(columnCount int) (*keymap.KeyMap, []string, []any) {
	columns := make([]string, columnCount)
	values := make([]any, columnCount)
	for i := range columns {
		columns[i] = fmt.Sprintf("col_%d", i)
		values[i] = i
	}

	return keymap.New(columns...), columns, values
}
