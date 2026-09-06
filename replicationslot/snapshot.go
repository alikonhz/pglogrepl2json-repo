package replicationslot

import (
	"context"
	"sync"

	"github.com/jackc/pglogrepl"
)

type Snapshot struct {
	LSN          pglogrepl.LSN
	SnapshotName string

	closeFn   func(context.Context) error
	closeOnce sync.Once
	closeErr  error
}

func NewSnapshot(lsn pglogrepl.LSN, snapshotName string, closeFn func(context.Context) error) *Snapshot {
	return &Snapshot{
		LSN:          lsn,
		SnapshotName: snapshotName,
		closeFn:      closeFn,
	}
}

func (s *Snapshot) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}

	s.closeOnce.Do(func() {
		if s.closeFn != nil {
			s.closeErr = s.closeFn(ctx)
		}
	})

	return s.closeErr
}
