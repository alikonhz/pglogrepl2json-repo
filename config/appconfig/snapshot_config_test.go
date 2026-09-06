package appconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotConfigValidateRequiresTableName(t *testing.T) {
	cfg := &SnapshotConfig{
		Mode: ModeOneTime,
		Config: []*SnapshotTableConfig{
			{Type: SnapshotTableFull},
		},
	}

	err := cfg.Validate()

	require.ErrorContains(t, err, "snapshot table name must not be empty")
}

func TestSnapshotConfigValidateRejectsDuplicateTableNames(t *testing.T) {
	cfg := &SnapshotConfig{
		Mode: ModeOneTime,
		Config: []*SnapshotTableConfig{
			{Name: "public.users", Type: SnapshotTableFull},
			{Name: "public.users", Type: SnapshotTableQuery, Query: "id > 0"},
		},
	}

	err := cfg.Validate()

	require.ErrorContains(t, err, `snapshot table "public.users" is configured more than once`)
}
