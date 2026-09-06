package orderedmap

import (
	"math"
	"testing"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/stretchr/testify/require"
)

func TestMarshalJSON(t *testing.T) {
	keyMap := keymap.New("id", "value", "value2")
	m := New(keyMap)

	m.Set("id", 1)
	m.Set("value", "value")
	m.Set("value2", float32(1.23))

	expected := `{"id":1,"value":"value","value2":1.23}`

	j, err := m.MarshalJSON()
	require.NoError(t, err)

	actual := string(j)

	require.Equal(t, expected, actual)
}

func TestMarshalJSONDuplicateKey(t *testing.T) {
	keyMap := keymap.New("id")
	m := New(keyMap)

	m.Set("id", 1)
	m.Set("id", 2)

	expected := `{"id":2}`

	j, err := m.MarshalJSON()
	require.NoError(t, err)

	actual := string(j)

	require.Equal(t, expected, actual)
}

func TestMarshalJSONSkipsUnsetKeys(t *testing.T) {
	keyMap := keymap.New("id", "commit_time")
	m := New(keyMap)

	m.Set("id", 1)

	j, err := m.MarshalJSON()
	require.NoError(t, err)

	require.Equal(t, `{"id":1}`, string(j))
	require.Equal(t, uint32(1), m.Size())

	_, ok := m.Get("commit_time")
	require.False(t, ok)
}

func TestMarshalJSONIncludesKnownKeySetLater(t *testing.T) {
	keyMap := keymap.New("id", "commit_time")
	m := New(keyMap)

	m.Set("id", 1)
	m.Set("commit_time", "2026-05-14T10:00:00Z")

	j, err := m.MarshalJSON()
	require.NoError(t, err)

	require.Equal(t, `{"id":1,"commit_time":"2026-05-14T10:00:00Z"}`, string(j))
	require.Equal(t, uint32(2), m.Size())
}

func TestClearResetsState(t *testing.T) {
	keyMap := keymap.New("id", "name")
	m := New(keyMap)

	m.Set("id", 1)
	m.Set("name", "Alice")
	m.Clear()

	require.Equal(t, uint32(0), m.Size())

	j, err := m.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `{}`, string(j))

	_, ok := m.Get("id")
	require.False(t, ok)
}

func TestMapWorksWhenIncorrectSizeIsPassed(t *testing.T) {
	km := keymap.New("key1", "key2")
	m := New(km)
	m.Set("key1", 1)
	m.Set("key2", 2)

	expected := `{"key1":1,"key2":2}`

	j, err := m.MarshalJSON()
	require.NoError(t, err)

	actual := string(j)
	require.Equal(t, expected, actual)
}

func TestCustomMarshalJSON(t *testing.T) {
	km := keymap.New("id", "name")
	m := New(km)

	m.Set("id", 1)
	m.Set("name", "John Doe")

	j, err := m.CustomMarshalJSON(
		[]string{"name", "missing", "id"},
		[]ValuePair{{Key: "commit_time", Value: "2026-05-13T10:00:00Z"}},
	)
	require.NoError(t, err)

	require.Equal(t, `{"name":"John Doe","id":1,"commit_time":"2026-05-13T10:00:00Z"}`, string(j))
}

func TestMarshalJSONReturnsErrorForUnsupportedValue(t *testing.T) {
	km := keymap.New("bad")
	m := New(km)
	m.Set("bad", make(chan int))

	j, err := m.MarshalJSON()
	require.Error(t, err)
	require.Nil(t, j)
}

func TestSetPanicsWhenKeyIsMissingFromKeyMap(t *testing.T) {
	km := keymap.New("id")
	m := New(km)

	require.Panics(t, func() {
		m.Set("missing", 1)
	})
}

func TestMarshalJSONReturnsErrorForNonFiniteFloat(t *testing.T) {
	km := keymap.New("bad")
	m := New(km)
	m.Set("bad", math.Inf(1))

	j, err := m.MarshalJSON()
	require.Error(t, err)
	require.Nil(t, j)
}

func TestCustomMarshalJSONReturnsErrorForUnsupportedSelectedValue(t *testing.T) {
	km := keymap.New("bad")
	m := New(km)
	m.Set("bad", func() {})

	j, err := m.CustomMarshalJSON([]string{"bad"}, nil)
	require.Error(t, err)
	require.Nil(t, j)
}

func TestCustomMarshalJSONReturnsErrorForUnsupportedCustomValue(t *testing.T) {
	km := keymap.New("key1")
	m := New(km)

	j, err := m.CustomMarshalJSON(nil, []ValuePair{{Key: "bad", Value: make(chan int)}})
	require.Error(t, err)
	require.Nil(t, j)
}
