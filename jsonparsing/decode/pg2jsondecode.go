package decode

import (
	"bytes"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	_NaN = []byte("NaN")
)

func Decode(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	if decoder, exists := dataTypeDecoders[dataType]; exists {
		return decoder(data, dataType, typeMap, encoding)
	}

	return decodeFromTypeMap(data, dataType, typeMap)
}

type pgDecoder func([]byte, uint32, *pgtype.Map, EncodingFormat) (any, error)

func decodeFromTypeMap(data []byte, dataType uint32, typeMap *pgtype.Map) (any, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		v, err := tryDecodeValue(data, dataType, typeMap, dt)
		if err != nil {
			return nil, fmt.Errorf("failed to decode data type %d: %w", dataType, err)
		}

		return v, nil
	}

	return string(data), nil
}

func tryDecodeValue(data []byte, dataType uint32, typeMap *pgtype.Map, dt *pgtype.Type) (any, error) {
	v, err := dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	if err != nil {
		// we got NaN
		if len(data) == len(_NaN) && bytes.Equal(data, _NaN) {
			return math.NaN(), nil
		}

		return nil, err
	}

	return v, nil
}

func stringDecoder(data []byte, _ uint32, _ *pgtype.Map, _ EncodingFormat) (any, error) {
	return string(data), nil
}

var (
	dataTypeDecoders map[uint32]pgDecoder = map[uint32]pgDecoder{
		pgtype.IntervalOID:       stringDecoder,
		pgtype.IntervalArrayOID:  stringDecoder,
		pgtype.PointOID:          stringDecoder,
		pgtype.PointArrayOID:     stringDecoder,
		pgtype.LsegOID:           stringDecoder,
		pgtype.LsegArrayOID:      stringDecoder,
		pgtype.CircleOID:         stringDecoder,
		pgtype.CircleArrayOID:    stringDecoder,
		pgtype.PathOID:           stringDecoder,
		pgtype.PathArrayOID:      stringDecoder,
		pgtype.BoxOID:            stringDecoder,
		pgtype.BoxArrayOID:       stringDecoder,
		pgtype.LineOID:           stringDecoder,
		pgtype.LineArrayOID:      stringDecoder,
		pgtype.PolygonOID:        stringDecoder,
		pgtype.PolygonArrayOID:   stringDecoder,
		pgtype.Macaddr8OID:       stringDecoder,
		pgtype.MacaddrOID:        stringDecoder,
		pgtype.MacaddrArrayOID:   stringDecoder,
		pgtype.InetOID:           stringDecoder,
		pgtype.InetArrayOID:      stringDecoder,
		pgtype.CIDROID:           stringDecoder,
		pgtype.CIDRArrayOID:      stringDecoder,
		pgtype.BitOID:            stringDecoder,
		pgtype.BitArrayOID:       stringDecoder,
		pgtype.JSONOID:           stringDecoder,
		pgtype.JSONArrayOID:      stringDecoder,
		pgtype.XMLOID:            stringDecoder,
		pgtype.XMLArrayOID:       stringDecoder,
		pgtype.JSONBOID:          stringDecoder,
		pgtype.JSONBArrayOID:     stringDecoder,
		pgtype.JSONPathOID:       stringDecoder,
		pgtype.JSONPathArrayOID:  stringDecoder,
		pgtype.UUIDOID:           stringDecoder,
		pgtype.UUIDArrayOID:      stringDecoder,
		pgtype.Int2ArrayOID:      stringDecoder,
		pgtype.Int4ArrayOID:      stringDecoder,
		pgtype.Int8ArrayOID:      stringDecoder,
		pgtype.TextArrayOID:      stringDecoder,
		pgtype.Int4rangeOID:      stringDecoder,
		pgtype.Int4rangeArrayOID: stringDecoder,
		pgtype.Int8rangeOID:      stringDecoder,
		pgtype.Int8rangeArrayOID: stringDecoder,
		pgtype.NumrangeOID:       stringDecoder,
		pgtype.NumrangeArrayOID:  stringDecoder,
		pgtype.TsrangeOID:        stringDecoder,
		pgtype.TsrangeArrayOID:   stringDecoder,
		pgtype.TstzrangeOID:      stringDecoder,
		pgtype.TstzrangeArrayOID: stringDecoder,
		pgtype.DaterangeOID:      stringDecoder,
		pgtype.DaterangeArrayOID: stringDecoder,

		pgtype.Float4OID:      float4Decoder,
		pgtype.Float8OID:      float8Decoder,
		pgtype.NumericOID:     numericDecoder,
		pgtype.ByteaOID:       byteDecoder,
		pgtype.DateOID:        timestampDecoderToDate,
		pgtype.TimestamptzOID: timestampDecoderToDateTime,
		pgtype.TimestampOID:   timestampDecoderToDateTime,
		pgtype.TimeOID:        timeDecoder,
		pgtype.TimetzOID:      stringDecoder,
		pgtype.BoolOID:        boolDecoder,
	}
)

func unexpectedTypeErr(decoderName string, dataType uint32, v any) error {
	return fmt.Errorf("%s: unexpected type for OID %d. type was %T", decoderName, dataType, v)
}
