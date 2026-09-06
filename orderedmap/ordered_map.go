package orderedmap

import (
	"iter"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/bytedance/sonic/encoder"
)

// OrderedMap represents a map, which returns keys in the same order they were added.
// NOT thread safe.
type OrderedMap struct {
	values []any
	set    []bool
	size   uint32
	keyMap *keymap.KeyMap
}

type ValuePair struct {
	Key   string
	Value any
}

func New(keyMap *keymap.KeyMap) *OrderedMap {
	return &OrderedMap{
		values: make([]any, keyMap.Len()),
		set:    make([]bool, keyMap.Len()),
		keyMap: keyMap,
	}
}

func (om *OrderedMap) Release() {
	// DO NOTHING
}

func (om *OrderedMap) Set(k string, v any) {
	index := om.keyMap.GetIndex(k)
	if index < 0 {
		panic("(Set) key not found in keymap")
	}
	if index >= om.keyMap.Len() {
		panic("(Set) key index out of range")
	}

	if !om.set[index] {
		om.set[index] = true
		om.size++
	}

	om.values[index] = v
}

func (om *OrderedMap) Get(k string) (any, bool) {
	index := om.keyMap.GetIndex(k)
	if index < 0 {
		return nil, false
	}
	if !om.set[index] {
		return nil, false
	}

	return om.values[index], true
}

func (om *OrderedMap) GetValue(k string) any {
	index := om.keyMap.GetIndex(k)
	if index < 0 {
		panic("(Get) key not found in keymap")
	}
	if !om.set[index] {
		return nil
	}

	return om.values[index]
}

func (om *OrderedMap) Size() uint32 {
	return om.size
}

func (om *OrderedMap) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, estimateJSONSize(int(om.size), 0))
	buf = append(buf, '{')

	wroteValue := false
	for i, key := range om.keyMap.Keys() {
		if !om.set[i] {
			continue
		}

		if wroteValue {
			buf = append(buf, ',')
		}

		if err := encoder.EncodeInto(&buf, key, encoder.NoEncoderNewline); err != nil {
			return nil, err
		}

		buf = append(buf, ':')

		if err := encoder.EncodeInto(&buf, om.values[i], encoder.NoEncoderNewline); err != nil {
			return nil, err
		}
		wroteValue = true
	}

	buf = append(buf, '}')

	return buf, nil
}

func (om *OrderedMap) CustomMarshalJSON(keys []string, custom []ValuePair) ([]byte, error) {
	if keys == nil {
		keys = om.keyMap.Keys()
	}

	buf := make([]byte, 0, estimateJSONSize(len(keys), len(custom)))
	buf = append(buf, '{')

	wroteValue := false
	for _, key := range keys {
		keyIndex := om.keyMap.GetIndex(key)
		if keyIndex < 0 {
			continue
		}
		if !om.set[keyIndex] {
			continue
		}

		if wroteValue {
			buf = append(buf, ',')
		}

		if err := encoder.EncodeInto(&buf, key, encoder.NoEncoderNewline); err != nil {
			return nil, err
		}

		buf = append(buf, ':')

		if err := encoder.EncodeInto(&buf, om.values[keyIndex], encoder.NoEncoderNewline); err != nil {
			return nil, err
		}
		wroteValue = true
	}

	if len(custom) > 0 {
		for _, valuePair := range custom {
			if wroteValue {
				buf = append(buf, ',')
			}

			if err := encoder.EncodeInto(&buf, valuePair.Key, encoder.NoEncoderNewline); err != nil {
				return nil, err
			}

			buf = append(buf, ':')

			if err := encoder.EncodeInto(&buf, valuePair.Value, encoder.NoEncoderNewline); err != nil {
				return nil, err
			}
			wroteValue = true
		}
	}

	buf = append(buf, '}')

	return buf, nil
}

func estimateJSONSize(keyCount int, customCount int) int {
	const estimatedPairSize = 32

	return 2 + (keyCount+customCount)*estimatedPairSize
}

func (om *OrderedMap) Iter() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		for index, key := range om.keyMap.Keys() {
			if !om.set[index] {
				continue
			}

			if !yield(key, om.values[index]) {
				return
			}
		}
	}
}

func (om *OrderedMap) Clear() {
	for i, set := range om.set {
		if set {
			om.set[i] = false
			om.values[i] = nil
		}
	}
	om.size = 0
}
