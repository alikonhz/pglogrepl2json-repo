package pg2redis

import (
	"context"

	"github.com/alikonhz/pglogrepl2json/pg2buffer"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

type redisSliceWriter struct {
	redisWriter
	writes       []*pgwal.WriteRequest
	lastSavedLSN pglogrepl.LSN
}

func (r *redisSliceWriter) start(_ context.Context) {
}

func (r *redisSliceWriter) gracefulShutdown() error {
	return nil
}

func (r *redisSliceWriter) close(_ context.Context) error {
	return nil
}

func (r *redisSliceWriter) enqueue(_ context.Context, req *pgwal.WriteRequest, responseTracker pg2buffer.ResponseTracker) error {
	r.writes = append(r.writes, req)

	respEntries := make([]*pgwal.ResponseEntry, len(req.Entries))
	for i, entry := range req.Entries {
		// release tuples for possible reusing
		entry.Release()

		respEntries[i] = &pgwal.ResponseEntry{
			PK:     entry.PK,
			Table:  entry.Table,
			LSN:    req.LSN,
			XID:    req.XID,
			Offset: entry.Offset,
		}
	}

	if len(respEntries) == 0 {
		respEntries = append(respEntries, &pgwal.ResponseEntry{
			LSN: req.LSN,
			XID: req.XID,
		})
	}

	responseTracker.OnSuccess(&pgwal.Response{Entries: respEntries})

	return nil
}

func (r *redisSliceWriter) saveLSN(_ context.Context, lsn pglogrepl.LSN) error {
	r.lastSavedLSN = lsn
	return nil
}

func (r *redisSliceWriter) SaveSnapshotLSN(_ context.Context, key string, lsn string) error {
	return nil
}

func (r *redisSliceWriter) GetSnapshotLSN(_ context.Context, key string) (string, error) {
	return "", nil
}

func (r *redisSliceWriter) readLSN(_ context.Context) (string, error) {
	return r.lastSavedLSN.String(), nil
}

func (r *redisSliceWriter) notEmptyWrites() []*pgwal.WriteRequest {
	var notEmptyWrites []*pgwal.WriteRequest

	for _, write := range r.writes {
		if len(write.Entries) > 0 {
			notEmptyWrites = append(notEmptyWrites, write)
		}
	}

	return notEmptyWrites
}

func (r *redisSliceWriter) ping(_ context.Context) error {
	return nil
}

func (r *redisSliceWriter) GetStatsAsFields() []zap.Field {
	return nil
}
