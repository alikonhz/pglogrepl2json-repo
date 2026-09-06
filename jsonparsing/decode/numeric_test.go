package decode

import (
	"fmt"
	"math"
	"testing"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalJSON_PgTypeNumeric(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{name: "NaN", value: "NaN", expected: `{"value":null}`},
		{name: "1.23", value: "1.23", expected: `{"value":1.23}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pgt := pgtype.Numeric{}
			err := pgt.ScanScientific(test.value)
			require.NoError(t, err)

			m := orderedmap.New(keymap.New("value"))

			m.Set("value", coercePgTypeNumeric(pgt))

			js, err := m.MarshalJSON()
			require.NoError(t, err)

			assert.Equal(t, test.expected, string(js), "pgtype.Numeric")
		})
	}
}

func TestMarshalJSON_Float64(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		expected string
	}{
		{name: "NaN", value: math.NaN(), expected: `{"value":null}`},
		{name: "+Infinity", value: math.Inf(1), expected: fmt.Sprintf(`{"value":%g}`, math.MaxFloat64)},
		{name: "-Infinity", value: math.Inf(-1), expected: fmt.Sprintf(`{"value":%g}`, math.SmallestNonzeroFloat64)},
		{name: "1.23", value: 1.23, expected: `{"value":1.23}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f64 := SmartFloat64(test.value)
			js64Map := orderedmap.New(keymap.New("value"))

			js64Map.Set("value", f64)

			js64, err := js64Map.MarshalJSON()
			require.NoError(t, err)

			assert.Equal(t, test.expected, string(js64), "js64")
		})
	}
}

func TestMarshalJSON_Float32(t *testing.T) {
	tests := []struct {
		name     string
		value    float32
		expected string
	}{
		{name: "NaN", value: float32(math.NaN()), expected: `{"value":null}`},
		{name: "+Infinity", value: float32(math.Inf(1)), expected: fmt.Sprintf(`{"value":%g}`, math.MaxFloat32)},
		{name: "-Infinity", value: float32(math.Inf(-1)), expected: fmt.Sprintf(`{"value":%g}`, math.SmallestNonzeroFloat32)},
		{name: "1.23", value: 1.23, expected: `{"value":1.23}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f32 := SmartFloat32(test.value)

			js32Map := orderedmap.New(keymap.New("value"))

			js32Map.Set("value", f32)

			js32, err := js32Map.MarshalJSON()
			require.NoError(t, err)

			assert.Equal(t, test.expected, string(js32), "js32")
		})
	}
}
