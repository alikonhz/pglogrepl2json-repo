package jsonparsing

import (
	"fmt"

	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func ReadTuple(t *pglogrepl.TupleData,
	columns []*pglogrepl.RelationMessageColumn,
	typeMap *pgtype.Map,
	cfg decode.ReadTupleConfig,
	keyMap *keymap.KeyMap) (*orderedmap.OrderedMap, error) {

	if t == nil {
		return nil, nil
	}

	if keyMap == nil {
		return nil, fmt.Errorf("read tuple: keymap is nil")
	}

	res := orderedmap.New(keyMap)

	if cfg.ReadAll || cfg.ReadIndexes == nil {
		for i := 0; i < len(t.Columns); i++ {
			relCol := columns[i]
			if keyMap.GetIndex(relCol.Name) < 0 {
				continue
			}

			col := t.Columns[i]
			err := writeValue(res, col, relCol, typeMap, cfg.Encoding)
			if err != nil {
				return nil, err
			}
		}

		return res, nil
	}

	for _, index := range cfg.ReadIndexes {
		relCol := columns[index]
		if keyMap.GetIndex(relCol.Name) < 0 {
			continue
		}

		col := t.Columns[index]
		err := writeValue(res, col, relCol, typeMap, cfg.Encoding)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func writeValue(res *orderedmap.OrderedMap,
	col *pglogrepl.TupleDataColumn,
	relCol *pglogrepl.RelationMessageColumn,
	typeMap *pgtype.Map,
	encoding decode.EncodingFormat) error {

	switch col.DataType {
	case 'n': // null
		res.Set(relCol.Name, nil)
	case 'u': // unchanged toast
		// do nothing
	case 't':
		val, err := decode.Decode(col.Data, relCol.DataType, typeMap, encoding)
		if err != nil {
			return fmt.Errorf("writeValue: failed to decode value for column %s with OID %d: %w", relCol.Name, relCol.DataType, err)
		}

		res.Set(relCol.Name, val)
		//return fmt.Errorf("failed to read column %s with type OID %d from PG stream. type was %T", relCol.Name, relCol.DataType, val)
	}

	return nil
}
