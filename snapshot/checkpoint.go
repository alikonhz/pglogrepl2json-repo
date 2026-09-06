package snapshot

import (
	"context"

	"github.com/alikonhz/pglogrepl2json"
)

// CheckpointManager handles snapshot LSN persistence
type CheckpointManager struct {
	listener pglogrepl2json.SnapshotListener
}

func NewCheckpointManager(listener pglogrepl2json.SnapshotListener) *CheckpointManager {
	return &CheckpointManager{
		listener: listener,
	}
}

func (c *CheckpointManager) SaveLSN(ctx context.Context, lsn string) error {
	return c.listener.SaveSnapshotLSN(ctx, lsn)
}

func (c *CheckpointManager) GetLSN(ctx context.Context) (string, error) {
	return c.listener.GetSnapshotLSN(ctx)
}
