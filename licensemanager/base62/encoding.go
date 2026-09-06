package base62

import (
	"fmt"
	"math/big"
)

const (
	base = 62
)

func EncodeToString(data []byte) (string, error) {
	var i big.Int

	i.SetBytes(data)

	return i.Text(base), nil
}

func DecodeString(s string) ([]byte, error) {
	var i big.Int
	_, ok := i.SetString(s, base)

	if !ok {
		return nil, fmt.Errorf("cannot parse base62")
	}

	return i.Bytes(), nil
}
