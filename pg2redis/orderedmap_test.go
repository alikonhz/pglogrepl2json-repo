package pg2redis

import (
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
)

func newTestOrderedMap(keys ...string) *orderedmap.OrderedMap {
	return orderedmap.New(keymap.New(keys...))
}
