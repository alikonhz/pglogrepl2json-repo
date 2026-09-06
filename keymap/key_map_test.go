package keymap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyMapReturnsIndexes(t *testing.T) {
	km := New("id", "name", "created_at")

	require.Equal(t, 0, km.GetIndex("id"))
	require.Equal(t, 1, km.GetIndex("name"))
	require.Equal(t, 2, km.GetIndex("created_at"))
}

func TestKeyMapReturnsMinusOneForMissingKey(t *testing.T) {
	km := New("id")

	require.Equal(t, -1, km.GetIndex("missing"))
}

func TestKeyMapHandlesEmptyInput(t *testing.T) {
	km := New()

	require.Equal(t, -1, km.GetIndex("1"))
}

func TestKeyMapDuplicateKeysUseLastIndex(t *testing.T) {
	km := New("id", "name", "id")

	require.Equal(t, 2, km.GetIndex("id"))
	require.Equal(t, 1, km.GetIndex("name"))
	require.Equal(t, 3, km.Len())
}
