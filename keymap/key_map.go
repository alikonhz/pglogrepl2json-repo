package keymap

type KeyMap struct {
	keysMap map[string]int
	keysArr []string
	len     int
}

func New(keys ...string) *KeyMap {
	keysMap := make(map[string]int, len(keys))
	keysArr := make([]string, len(keys))
	for index, key := range keys {
		keysMap[key] = index
		keysArr[index] = key
	}

	return &KeyMap{
		keysMap: keysMap,
		keysArr: keysArr,
		len:     len(keys),
	}
}

func (km *KeyMap) GetIndex(key string) int {
	index, ok := km.keysMap[key]
	if !ok {
		return -1
	}

	return index
}

func (km *KeyMap) Len() int {
	return km.len
}

func (km *KeyMap) Keys() []string {
	return km.keysArr
}
