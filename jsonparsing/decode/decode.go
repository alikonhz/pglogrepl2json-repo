package decode

import (
	"encoding/base64"
	"encoding/hex"
)

type BinaryEncoding byte

const (
	// BinaryEncodingBase64 specifies that all []byte data should be encoded as base64 string. This is the default value
	BinaryEncodingBase64 BinaryEncoding = 0
	// BinaryEncodingHex specifies that all []byte data should be encoded as HEX string
	BinaryEncodingHex BinaryEncoding = 1
)

type NumericEncoding byte

const (
	NumericEncodingFloat  NumericEncoding = 0 // float
	NumericEncodingString NumericEncoding = 1 // string

	NumericEncodingDefault NumericEncoding = NumericEncodingFloat
)

type EncodingFormat struct {
	Binary  BinaryEncoding
	Numeric NumericEncoding
}

type ReadTupleConfig struct {
	ReadIndexes []int
	ReadAll     bool
	Encoding    EncodingFormat
}

func EncodeBinary(data []byte, binaryFormat BinaryEncoding) string {
	switch binaryFormat {
	case BinaryEncodingHex:
		return hex.EncodeToString(data)
	default:
		return base64.StdEncoding.EncodeToString(data)
	}
}
