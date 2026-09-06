package decode

import (
	"math"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/jackc/pgx/v5/pgtype"
)

//strconv.FormatFloat(..., 'g', -1, 64) means:
//'g' → Use either decimal or scientific notation, whichever is shorter, and remove trailing zeros.
//-1 → Use as many digits as needed, but no more.
//64 → Format using full precision of a float64.
//This is how tools like jq, JavaScript, and many JSON parsers handle float encoding.

type SmartFloat32 float32

func (f SmartFloat32) MarshalJSON() ([]byte, error) {
	switch {
	case math.IsNaN(float64(f)):
		return sonic.Marshal(nil)
	case math.IsInf(float64(f), 1):
		return sonic.Marshal(math.MaxFloat32)
	case math.IsInf(float64(f), -1):
		return sonic.Marshal(math.SmallestNonzeroFloat32)
	default:
		// Normal formatting
		s := strconv.FormatFloat(float64(f), 'g', -1, 32)

		return []byte(s), nil
	}
}

type SmartFloat64 float64

func (f SmartFloat64) MarshalJSON() ([]byte, error) {
	switch {
	case math.IsNaN(float64(f)):
		return sonic.Marshal(nil)
	case math.IsInf(float64(f), 1):
		return sonic.Marshal(math.MaxFloat64)
	case math.IsInf(float64(f), -1):
		return sonic.Marshal(math.SmallestNonzeroFloat64)
	default:
		// Normal formatting
		s := strconv.FormatFloat(float64(f), 'g', -1, 64)
		return []byte(s), nil
	}
}

func float4Decoder(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	return pgxGenericFloatDecoder[float32](data, dataType, typeMap, encoding.Numeric)
}

func float8Decoder(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	return pgxGenericFloatDecoder[float64](data, dataType, typeMap, encoding.Numeric)
}

func numericDecoder(data []byte, dataType uint32, typeMap *pgtype.Map, encoding EncodingFormat) (any, error) {
	return pgxGenericNumericDecoder(data, dataType, typeMap, encoding.Numeric)
}

func pgxGenericFloatDecoder[T float32 | float64](data []byte, dataType uint32, typeMap *pgtype.Map, numEncoding NumericEncoding) (any, error) {
	if numEncoding == NumericEncodingString {
		return string(data), nil
	}

	v, err := decodeFromTypeMap(data, dataType, typeMap)
	if err != nil {
		return nil, err
	}

	f, ok := v.(T)
	if !ok {
		return nil, unexpectedTypeErr("f32/f64 decoder", dataType, v)
	}

	return coerceFloat(f), nil
}

func pgxGenericNumericDecoder(data []byte, dataType uint32, typeMap *pgtype.Map, numEncoding NumericEncoding) (any, error) {
	if numEncoding == NumericEncodingString {
		return string(data), nil
	}

	v, err := decodeFromTypeMap(data, dataType, typeMap)
	if err != nil {
		return nil, err
	}

	f, ok := v.(pgtype.Numeric)
	if !ok {
		return nil, unexpectedTypeErr("numeric decoder", dataType, v)
	}

	return coercePgTypeNumeric(f), nil
}

func coercePgTypeNumeric(f pgtype.Numeric) pgtype.Numeric {
	// NaN and not valid convert to not valid
	// this will be "null" in json
	if f.NaN || !f.Valid {
		return pgtype.Numeric{Valid: false}
	}

	return f
}

func coerceFloat[T float32 | float64](f T) any {
	if math.IsNaN(float64(f)) {
		return nil
	}

	var zero T
	switch any(zero).(type) {
	case float32:
		return SmartFloat32(f)
	default:
		return SmartFloat64(f)
	}
}

//func coerceFloat32(f float32) any {
//	if math.IsInf(float64(f), -1) {
//		return math.SmallestNonzeroFloat32
//	}
//
//	if math.IsInf(float64(f), 1) {
//		return float32(math.MaxFloat32)
//	}
//
//	return SmartFloat32(f)
//}
//
//func coerceFloat64(f float64) any {
//	if math.IsInf(f, -1) {
//		return math.SmallestNonzeroFloat64
//	}
//
//	if math.IsInf(f, 1) {
//		return math.MaxFloat64
//	}
//
//	return SmartFloat64(f)
//}
