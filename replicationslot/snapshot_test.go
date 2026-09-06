package replicationslot

import (
	"context"
	"testing"
)

func TestSnapshotCloseIsIdempotent(t *testing.T) {
	closeCalls := 0
	snapshot := NewSnapshot(1, "snapshot-1", func(context.Context) error {
		closeCalls++
		return nil
	})

	if err := snapshot.Close(context.Background()); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	if err := snapshot.Close(context.Background()); err != nil {
		t.Fatalf("second close failed: %v", err)
	}

	if closeCalls != 1 {
		t.Fatalf("close called %d times, want 1", closeCalls)
	}
}
