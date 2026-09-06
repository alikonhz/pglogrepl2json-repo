package decode

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
)

func byteDecoder(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	v, err := decodeFromTypeMap(data, dataType, typeMap)
	if err != nil {
		return nil, fmt.Errorf("failed to decode bytea: %w", err)
	}

	d, ok := v.([]byte)
	if !ok {
		return nil, unexpectedTypeErr("byte decoder", dataType, v)
	}

	if len(d) > 0 {
		return EncodeBinary(d, encoding.Binary), nil
	}

	return nil, nil
}
