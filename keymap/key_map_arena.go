package keymap

import "sync"

var (
	GlobalKeyMapStringArena = NewArena()
)

type Arena struct {
	mu      sync.RWMutex
	allMaps map[arenaKey]*KeyMap
}

type arenaKey struct {
	name    string
	version string
}

func NewArena() *Arena {
	return &Arena{
		allMaps: make(map[arenaKey]*KeyMap),
	}
}

func (a *Arena) Add(name, version string, km *KeyMap) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := arenaKey{name: name, version: version}
	a.allMaps[key] = km
}

func (a *Arena) Get(name, version string) *KeyMap {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := arenaKey{name: name, version: version}
	return a.allMaps[key]
}
