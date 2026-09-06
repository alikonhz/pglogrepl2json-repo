package listeners

import (
	"context"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
)

// NoSnapshotListener is a noop implementation of SnapshotListener
type NoSnapshotListener struct {
}

func NewNoSnapshotListener() *NoSnapshotListener {
	return &NoSnapshotListener{}
}

func (l *NoSnapshotListener) OnSnapshotBatch(ctx context.Context, tableName pgschema.TableName, batch *pgwal.WriteRequest) error {
	return nil
}

func (l *NoSnapshotListener) TxOptions() appconfig.TxCommitTimeOptions {
	return appconfig.NewTxCommitTimeOptions("")
}

func (l *NoSnapshotListener) SetSnapshotAckHandler(handler func(*pgwal.Response, pglogrepl2json.CommitPoint)) {
}

func (l *NoSnapshotListener) GetTable(tableName pgschema.TableName) *pgschema.Table {
	return nil
}

func (l *NoSnapshotListener) CommitPos() pglogrepl2json.CommitPoint {
	return pglogrepl2json.CommitPoint{}
}

func (l *NoSnapshotListener) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	return nil
}

func (l *NoSnapshotListener) GetSnapshotLSN(ctx context.Context) (string, error) {
	return "", nil
}

func (l *NoSnapshotListener) GracefulShutdown(ctx context.Context) error {
	return nil
}

func (l *NoSnapshotListener) WriteQueueSize() uint64 {
	return 0
}
