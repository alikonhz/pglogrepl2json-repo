package pglogrepl2json

import (
	"context"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/jackc/pglogrepl"
)

type CommitPoint struct {
	LSN pglogrepl.LSN
	XID uint32
}

// ReplicationListener is a target data store where Replicator sends logical decoding messages
type ReplicationListener interface {
	pg2stats.StatLogger

	// OnTxBegin is called when BeginMessage is received from replication connection
	OnTxBegin(msg *pglogrepl.BeginMessage) error

	// OnTxCommit is called when CommitMessage is received from replication connection
	OnTxCommit(msg *pglogrepl.CommitMessage) error

	// OnStreamStart is called when StreamStartMessageV2 is received from replication connection
	OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error

	// OnStreamStop is called when StreamStopMessageV2 is received from replication connection
	OnStreamStop(msg *pglogrepl.StreamStopMessageV2) error

	// OnStreamCommit is called when StreamCommitMessageV2 is received from replication connection
	OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error

	// OnStreamAbort is called when StreamAbortMessageV2 is received from replication connection
	OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error

	// OnInsert is called when InsertMessageV2 is received from replication connection
	OnInsert(msg *pglogrepl.InsertMessageV2) error

	// OnUpdate is called when UpdateMessageV2 is received from replication connection
	OnUpdate(msg *pglogrepl.UpdateMessageV2) error

	// OnDelete is called when DeleteMessageV2 is received from replication connection
	OnDelete(msg *pglogrepl.DeleteMessageV2) error

	// OnLogicalDecodingMessage is called when LogicalDecodingMessageV2 is received from replication connection
	OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error

	// OnTruncate is called when TruncateMessageV2 is received from replication connection
	OnTruncate(msg *pglogrepl.TruncateMessageV2) error

	// OnRelation is called when RelationMessageV2 is received from replication connection
	OnRelation(msg *pglogrepl.RelationMessageV2) error

	// OnOrigin is called when OriginMessage is received from replication connection
	OnOrigin(msg *pglogrepl.OriginMessage) error

	// OnType is called when TypeMessageV2 is received from replication connection
	OnType(msg *pglogrepl.TypeMessageV2) error

	CommitPos() CommitPoint

	PluginArguments(pubName string) ([]string, error)

	CoordinatedShutdownCh() <-chan error

	WriteQueueSize() uint64

	// GracefulShutdown shuts down all the activities and waits for all the workers to finish.
	// Implementations should not close downstream connections. Instead, this should be done in the Close method.
	GracefulShutdown(ctx context.Context) error

	SaveState(ctx context.Context, lsn pglogrepl.LSN) error

	// Close closes the listener. Implementations should close all the downstream connections.
	Close(ctx context.Context) error
}

type SnapshotListener interface {
	OnSnapshotBatch(ctx context.Context, tableName pgschema.TableName, batch *pgwal.WriteRequest) error
	SetSnapshotAckHandler(handler func(*pgwal.Response, CommitPoint))
	GetTable(tableName pgschema.TableName) *pgschema.Table
	CommitPos() CommitPoint
	SaveSnapshotLSN(ctx context.Context, lsn string) error
	GetSnapshotLSN(ctx context.Context) (string, error)
	GracefulShutdown(ctx context.Context) error
	WriteQueueSize() uint64
	TxOptions() appconfig.TxCommitTimeOptions
}
