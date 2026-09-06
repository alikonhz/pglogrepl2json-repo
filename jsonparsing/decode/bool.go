package decode

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
)

func boolDecoder(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	v, err := decodeFromTypeMap(data, dataType, typeMap)
	if err != nil {
		return nil, fmt.Errorf("failed to decode bool: %w", err)
	}

	t, ok := v.(bool)
	if !ok {
		return nil, unexpectedTypeErr("bool decoder", dataType, v)
	}

	return t, nil
}
