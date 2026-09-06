package entrypool

import (
	"iter"
	"sync"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pgwal"
)

type EntryTuplePool struct {
	pools *sync.Map
}

func NewPool() *EntryTuplePool {
	return &EntryTuplePool{
		pools: &sync.Map{},
	}
}

func (p *EntryTuplePool) Get(name string, version string, keyMap *keymap.KeyMap) pgwal.EntryTuple {
	key := name + "_" + version

	var targetPool *sync.Pool
	if v, ok := p.pools.Load(key); ok {
		targetPool = v.(*sync.Pool)
	} else {
		newPool := &sync.Pool{
			New: func() any {
				om := orderedmap.New(keyMap)
				return &pooledOrderedMap{om: om}
			},
		}

		v, _ := p.pools.LoadOrStore(key, newPool)
		targetPool = v.(*sync.Pool)
	}

	om := targetPool.Get().(*pooledOrderedMap)
	om.setOwner(targetPool)

	return om
}

type pooledOrderedMap struct {
	owner *sync.Pool
	om    *orderedmap.OrderedMap
}

func (om *pooledOrderedMap) setOwner(pool *sync.Pool) {
	om.owner = pool
}

func (om *pooledOrderedMap) Release() {
	owner := om.owner
	om.owner = nil
	om.om.Clear()
	owner.Put(om)
}

func (om *pooledOrderedMap) Get(key string) (any, bool) {
	return om.om.Get(key)
}

func (om *pooledOrderedMap) GetValue(key string) any {
	return om.om.GetValue(key)
}

func (om *pooledOrderedMap) Set(k string, v any) {
	om.om.Set(k, v)
}

func (om *pooledOrderedMap) Iter() iter.Seq2[string, any] {
	return om.om.Iter()
}

func (om *pooledOrderedMap) MarshalJSON() ([]byte, error) {
	return om.om.MarshalJSON()
}

func (om *pooledOrderedMap) CustomMarshalJSON(keys []string, custom []orderedmap.ValuePair) ([]byte, error) {
	return om.om.CustomMarshalJSON(keys, custom)
}

func (om *pooledOrderedMap) Size() uint32 {
	return om.om.Size()
}
