package snapshot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProgressUpdateStatement_StatusFailed(t *testing.T) {
	errorMessage := "snapshot failed"
	stateManager := &StateManager{appName: "test-app"}

	query, args, ok := stateManager.progressUpdateStatement("public.users", 0, StatusFailed, &errorMessage)

	require.True(t, ok)
	assert.Contains(t, compactSQL(query), "UPDATE pgwalk.snapshot_state SET status = $3, error_message = $4")
	assert.NotContains(t, compactSQL(query), "snapshot_end = NOW()")
	require.Len(t, args, 4)
	assert.Equal(t, "test-app", args[0])
	assert.Equal(t, "public.users", args[1])
	assert.Equal(t, StatusFailed, args[2])
	assert.Same(t, &errorMessage, args[3])
}

func TestProgressUpdateStatement_StatusCompletedWithProcessedRows(t *testing.T) {
	stateManager := &StateManager{appName: "test-app"}

	query, args, ok := stateManager.progressUpdateStatement("public.users", 42, StatusCompleted, nil)

	require.True(t, ok)
	assert.Contains(t, compactSQL(query), "processed_rows = $3, status = $4, error_message = $5")
	assert.Contains(t, compactSQL(query), "snapshot_end = NOW()")
	require.Len(t, args, 5)
	assert.Equal(t, "test-app", args[0])
	assert.Equal(t, "public.users", args[1])
	assert.Equal(t, int64(42), args[2])
	assert.Equal(t, StatusCompleted, args[3])
	assert.Nil(t, args[4])
}

func TestProgressUpdateStatement_StatusCompletedWithoutProcessedRows(t *testing.T) {
	stateManager := &StateManager{appName: "test-app"}

	query, args, ok := stateManager.progressUpdateStatement("public.empty", 0, StatusCompleted, nil)

	require.True(t, ok)
	assert.Contains(t, compactSQL(query), "UPDATE pgwalk.snapshot_state SET status = $3, error_message = $4")
	assert.Contains(t, compactSQL(query), "snapshot_end = NOW()")
	require.Len(t, args, 4)
	assert.Equal(t, "test-app", args[0])
	assert.Equal(t, "public.empty", args[1])
	assert.Equal(t, StatusCompleted, args[2])
	assert.Nil(t, args[3])
}

func TestCreateStateTableSQL_HasSnapshotStartEndColumns(t *testing.T) {
	compact := compactSQL(CreateStateTableSQL)

	assert.Contains(t, compact, "snapshot_start TIMESTAMP")
	assert.Contains(t, compact, "snapshot_end TIMESTAMP")
	assert.NotContains(t, compact, "ADD COLUMN IF NOT EXISTS")
}

func compactSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}
