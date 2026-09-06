package pg2runner

import (
	"testing"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/replicationslot"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSchema(t *testing.T) {
	pubRes := pgschema.PubTableRes{
		Tables: map[string]*pgschema.Table{
			"public.orders": {
				Name: pgschema.MakeTableName("public", "orders"),
				Columns: map[string]*pgschema.Column{
					"id":    {Name: "id"},
					"state": {Name: "state"},
				},
			},
		},
		TablesWithAllColumns: map[string]*pgschema.Table{
			"public.orders": {
				Name: pgschema.MakeTableName("public", "orders"),
				Columns: map[string]*pgschema.Column{
					"id":    {Name: "id"},
					"state": {Name: "state"},
					"other": {Name: "other"},
				},
			},
		},
	}

	t.Run("valid configuration", func(t *testing.T) {
		tablesConfig := map[string]*config.TableConfig{
			"public.orders": {
				Columns: []string{"id", "state"},
			},
		}
		err := validateSchema(pubRes, tablesConfig, "test_pub")
		assert.NoError(t, err)
	})

	t.Run("valid with all columns and extra validation", func(t *testing.T) {
		tablesConfig := map[string]*config.TableConfig{
			"public.orders": {
				Columns: []string{"all", "id", "state"},
			},
		}
		err := validateSchema(pubRes, tablesConfig, "test_pub")
		assert.NoError(t, err)
	})

	t.Run("invalid column in condition even with all columns", func(t *testing.T) {
		tablesConfig := map[string]*config.TableConfig{
			"public.orders": {
				Columns: []string{"all", "non_existent"},
			},
		}
		err := validateSchema(pubRes, tablesConfig, "test_pub")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `column "non_existent" for table "public.orders" is not in publication "test_pub"`)
	})

	t.Run("missing table", func(t *testing.T) {
		tablesConfig := map[string]*config.TableConfig{
			"public.missing": {
				Columns: []string{"id"},
			},
		}
		err := validateSchema(pubRes, tablesConfig, "test_pub")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "table public.missing is not in publication test_pub")
	})

	t.Run("column in physical table but not in publication", func(t *testing.T) {
		// "other" is in TablesWithAllColumns but not in Tables (explicit SELECT list)
		tablesConfig := map[string]*config.TableConfig{
			"public.orders": {
				Columns: []string{"other"},
			},
		}
		err := validateSchema(pubRes, tablesConfig, "test_pub")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `column "other" for table "public.orders" is not in publication "test_pub"`)
	})
}

func TestValidateCreatedSlotLSN(t *testing.T) {
	lsn, err := pglogrepl.ParseLSN("0/16B6C50")
	require.NoError(t, err)

	t.Run("skips check when slot was not created by app", func(t *testing.T) {
		assert.NoError(t, validateCreatedSlotLSN(nil, 0))
	})

	t.Run("accepts matching slot creation and confirmed flush LSNs", func(t *testing.T) {
		slotSnapshot := replicationslot.NewSnapshot(lsn, "snapshot-1", nil)

		assert.NoError(t, validateCreatedSlotLSN(slotSnapshot, lsn))
	})

	t.Run("rejects mismatch between slot creation and confirmed flush LSNs", func(t *testing.T) {
		slotSnapshot := replicationslot.NewSnapshot(lsn, "snapshot-1", nil)

		mismatchErr := validateCreatedSlotLSN(slotSnapshot, lsn+1)

		require.Error(t, mismatchErr)
		assert.Contains(t, mismatchErr.Error(), "created replication slot LSN 0/16B6C50 does not match confirmed_flush_lsn 0/16B6C51")
	})
}
