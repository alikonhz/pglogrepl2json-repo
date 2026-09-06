package pgschema

import (
	"testing"

	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePGVersionNum(t *testing.T) {
	t.Run("version is invalid or not supported", func(t *testing.T) {
		invalidVersions := []int{
			-2,
			0,
			110000,
			119999,
		}
		for _, version := range invalidVersions {
			ver, err := parsePGVersionNum(version)
			assert.Equal(t, Unknown, ver)
			assert.ErrorIs(t, err, ErrPGVersionNotSupported)
		}
	})
	t.Run("version is valid", func(t *testing.T) {
		validVersions := []struct {
			Version int
			PGVer   PGVersion
		}{
			{Version: 120000, PGVer: V12},
			{Version: 130000, PGVer: V13},
			{Version: 140012, PGVer: V14},
			{Version: 150005, PGVer: V15},
			{Version: 160003, PGVer: V16},
			{Version: 180000, PGVer: V18},
		}
		for _, version := range validVersions {
			ver, err := parsePGVersionNum(version.Version)
			require.NoError(t, err)
			assert.Equal(t, version.PGVer, ver)
		}
	})
}

func TestCreatePK_CompositeKeyUsesColumnPosition(t *testing.T) {
	table := NewTable("public", "orders")
	table.AddColumn(&Column{Name: "line_no", IsPK: true, Position: 3})
	table.AddColumn(&Column{Name: "order_id", IsPK: true, Position: 1})
	table.AddColumn(&Column{Name: "tenant_id", IsPK: true, Position: 2})

	tuple := orderedmap.New(keymap.New("line_no", "order_id", "tenant_id"))
	tuple.Set("line_no", 7)
	tuple.Set("order_id", 42)
	tuple.Set("tenant_id", "acme")

	assert.Equal(t, "42:acme:7", CreatePK(table, tuple))
}

func TestCreatePK_CompositeKeyFallsBackToNameForEqualPositions(t *testing.T) {
	table := NewTable("public", "orders")
	table.AddColumn(&Column{Name: "b_id", IsPK: true})
	table.AddColumn(&Column{Name: "a_id", IsPK: true})

	tuple := orderedmap.New(keymap.New("b_id", "a_id"))
	tuple.Set("b_id", 2)
	tuple.Set("a_id", 1)

	assert.Equal(t, "1:2", CreatePK(table, tuple))
}

func TestTableHasAllPhysicalColumns(t *testing.T) {
	table := NewTable("public", "orders")
	table.AddColumn(&Column{Name: "id"})
	table.AddColumn(&Column{Name: "status"})
	table.PhysicalColumnsCount = 2

	assert.True(t, table.HasAllPhysicalColumns())

	filtered := table.WithColumns(map[string]*Column{
		"id": table.Columns["id"],
	})

	assert.False(t, filtered.HasAllPhysicalColumns())
	assert.Equal(t, 2, filtered.PhysicalColumnsCount)
}
