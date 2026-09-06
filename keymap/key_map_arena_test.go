package keymap

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArenaStoresMapsByNameAndVersion(t *testing.T) {
	arena := NewArena()
	v1 := New("id")
	v2 := New("email")

	arena.Add("users", "1", v1)
	arena.Add("users", "2", v2)

	require.Same(t, v1, arena.Get("users", "1"))
	require.Same(t, v2, arena.Get("users", "2"))
	require.Nil(t, arena.Get("orders", "1"))
}

func TestArenaReplacesExistingMapForSameNameAndVersion(t *testing.T) {
	arena := NewArena()
	older := New("id")
	newer := New("name")

	arena.Add("users", "1", older)
	arena.Add("users", "1", newer)

	require.Same(t, newer, arena.Get("users", "1"))
	require.Equal(t, 0, arena.Get("users", "1").GetIndex("name"))
	require.Equal(t, -1, arena.Get("users", "1").GetIndex("id"))
}

func TestArenaDoesNotCollideWhenNameOrVersionContainsDelimiter(t *testing.T) {
	arena := NewArena()
	first := New("first")
	second := New("second")

	arena.Add("users||v1", "tenant", first)
	arena.Add("users", "v1||tenant", second)

	require.Same(t, first, arena.Get("users||v1", "tenant"))
	require.Same(t, second, arena.Get("users", "v1||tenant"))
}

func TestArenaAllowsConcurrentAccess(t *testing.T) {
	arena := NewArena()

	const (
		workers    = 16
		iterations = 100
	)

	errs := make(chan string, workers*iterations)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			name := fmt.Sprintf("table_%d", worker)
			for i := 0; i < iterations; i++ {
				version := fmt.Sprintf("v%d", i)
				km := New(fmt.Sprintf("key_%d_%d", worker, i))

				arena.Add(name, version, km)
				if got := arena.Get(name, version); got != km {
					errs <- fmt.Sprintf("got wrong map for %s/%s", name, version)
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errs)

	require.Empty(t, errs)
}
