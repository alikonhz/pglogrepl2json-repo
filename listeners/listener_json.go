package listeners

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/jsonparsing"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgwal"
	"github.com/bytedance/sonic"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

var (
	errRelationNotFound = "relation not found"
)

const (
	Begin        = "B"
	Insert       = "I"
	Update       = "U"
	Commit       = "c"
	Delete       = "D"
	Message      = "M"
	Truncate     = "T"
	StreamStart  = "AB"
	StreamStop   = "AS"
	StreamCommit = "AC"
	StreamAbort  = "AA"
)

const (
	XidKey           = "xid"
	SubXidKey        = "sub_xid"
	ActionKey        = "action"
	SchemaKey        = "schema"
	TableKey         = "table"
	ColumnsKey       = "columns"
	IdentityKey      = "identity"
	TimestampKey     = "timestamp"
	TransactionalKey = "transactional"
	ContentKey       = "content"
	PrefixKey        = "prefix"
	FirstSegmentKey  = "first_segment"

	TimestampFormat = time.RFC3339Nano
)

var (
	commit     = []byte(`{ "action": "` + Commit + `" }`)
	streamStop = []byte(`{ "action": "` + StreamStop + `" }`)
)

// ListenerJSONOptions contains options for JSON listener
type ListenerJSONOptions struct {
	// BinaryContentFormat controls how []byte data will be encoded. It can be either BinaryEncodingBase64 or BinaryEncodingHex.
	// If other value is set then BinaryEncodingBase64 is used
	BinaryContentFormat decode.BinaryEncoding
}

var (
	_ = pglogrepl2json.ReplicationListener((*ListenerJSON)(nil))
)

type relationVersion struct {
	version uint64
	rel     *pglogrepl.RelationMessageV2
	keyMap  *keymap.KeyMap
}

// ListenerJSON transforms output from PG replication into JSON format similar to wal2json plugin
type ListenerJSON struct {
	ListenerJSONOptions
	c                     chan []byte
	relations             map[uint32]*relationVersion
	typeMap               *pgtype.Map
	commitPos             pglogrepl.LSN
	commitXid             uint32
	currentXid            uint32
	forceCommit           bool
	coordinatedShutdownCh chan error

	arena           *keymap.Arena
	relationVersion uint64
}

// MustCreateNewJSON creates a new JSON listener
// it panics if c is nil
func MustCreateNewJSON(c chan []byte, opts ListenerJSONOptions) *ListenerJSON {
	if c == nil {
		panic("channel is nil")
	}
	tm := pgtype.NewMap()
	return &ListenerJSON{
		ListenerJSONOptions:   opts,
		c:                     c,
		relations:             make(map[uint32]*relationVersion),
		typeMap:               tm,
		coordinatedShutdownCh: make(chan error),
		arena:                 keymap.GlobalKeyMapStringArena,
	}
}

func (s *ListenerJSON) OnSnapshotBatch(ctx context.Context, tableName pgschema.TableName, batch *pgwal.WriteRequest) error {
	panic("not implemented")
}

func (s *ListenerJSON) TxOptions() appconfig.TxCommitTimeOptions {
	return appconfig.NewTxCommitTimeOptions(TimestampKey)
}

func (s *ListenerJSON) SetSnapshotAckHandler(handler func(*pgwal.Response, pglogrepl2json.CommitPoint)) {
}

func (s *ListenerJSON) GetTable(tableName pgschema.TableName) *pgschema.Table {
	panic("not implemented")
}

func (s *ListenerJSON) SaveSnapshotLSN(ctx context.Context, lsn string) error {
	panic("not implemented")
}

func (s *ListenerJSON) GetSnapshotLSN(ctx context.Context) (string, error) {
	panic("not implemented")
}

func (s *ListenerJSON) CoordinatedShutdownCh() <-chan error {
	return s.coordinatedShutdownCh
}

func (s *ListenerJSON) GetStatsAsFields() []zap.Field {
	return nil
}

func (s *ListenerJSON) GracefulShutdown(ctx context.Context) error {
	return nil
}

func (s *ListenerJSON) WriteQueueSize() uint64 {
	return 0
}

// OnOrigin is called when OriginMessage is received from replication connection
func (s *ListenerJSON) OnOrigin(_ *pglogrepl.OriginMessage) error {
	// not supported
	return nil
}

// OnTxBegin is called when BeginMessage is received from replication connection
func (s *ListenerJSON) OnTxBegin(msg *pglogrepl.BeginMessage) error {
	s.currentXid = msg.Xid

	m := make(map[string]any)
	m[ActionKey] = Begin
	m[XidKey] = msg.Xid

	m[TimestampKey] = msg.CommitTime.Format(TimestampFormat)

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnTxCommit is called when CommitMessage is received from replication connection
func (s *ListenerJSON) OnTxCommit(msg *pglogrepl.CommitMessage) error {

	s.c <- commit

	s.commitPos = msg.CommitLSN
	s.commitXid = s.currentXid
	s.forceCommit = true
	s.currentXid = 0

	return nil
}

// OnStreamStart is called when StreamStartMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamStart(msg *pglogrepl.StreamStartMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = StreamStart
	m[XidKey] = msg.Xid

	const firstSegment uint8 = 1
	m[FirstSegmentKey] = msg.FirstSegment == firstSegment

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnStreamStop is called when StreamStopMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamStop(_ *pglogrepl.StreamStopMessageV2) error {
	s.c <- streamStop
	return nil
}

// OnStreamCommit is called when StreamCommitMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamCommit(msg *pglogrepl.StreamCommitMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = StreamCommit
	m[XidKey] = msg.Xid
	m[TimestampKey] = msg.CommitTime.Format(TimestampFormat)

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	s.commitXid = msg.Xid
	s.commitPos = msg.CommitLSN
	s.forceCommit = true

	return nil
}

// OnStreamAbort is called when StreamAbortMessageV2 is received from replication connection
func (s *ListenerJSON) OnStreamAbort(msg *pglogrepl.StreamAbortMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = StreamAbort
	m[XidKey] = msg.Xid
	m[SubXidKey] = msg.SubXid

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}

	s.c <- j

	return nil
}

// OnInsert is called when InsertMessageV2 is received from replication connection
func (s *ListenerJSON) OnInsert(msg *pglogrepl.InsertMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Insert
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}
	m[SchemaKey] = rel.rel.Namespace
	m[TableKey] = rel.rel.RelationName
	v, err := jsonparsing.ReadTuple(msg.Tuple,
		rel.rel.Columns,
		s.typeMap,
		decode.ReadTupleConfig{
			ReadAll:  true,
			Encoding: decode.EncodingFormat{Binary: s.BinaryContentFormat},
		},
		rel.keyMap)
	if err != nil {
		return err
	}

	m[ColumnsKey] = v

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnUpdate is called when UpdateMessageV2 is received from replication connection
func (s *ListenerJSON) OnUpdate(msg *pglogrepl.UpdateMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Update
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}

	m[SchemaKey] = rel.rel.Namespace
	m[TableKey] = rel.rel.RelationName

	n, err := jsonparsing.ReadTuple(msg.NewTuple, rel.rel.Columns, s.typeMap, decode.ReadTupleConfig{
		ReadAll:  true,
		Encoding: decode.EncodingFormat{Binary: s.BinaryContentFormat},
	},
		rel.keyMap)
	if err != nil {
		return err
	}
	m[ColumnsKey] = n

	o, err := jsonparsing.ReadTuple(msg.OldTuple, rel.rel.Columns, s.typeMap, decode.ReadTupleConfig{
		ReadAll:  true,
		Encoding: decode.EncodingFormat{Binary: s.BinaryContentFormat},
	},
		rel.keyMap)
	if err != nil {
		return err
	}
	m[IdentityKey] = o

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnDelete is called when DeleteMessageV2 is received from replication connection
func (s *ListenerJSON) OnDelete(msg *pglogrepl.DeleteMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Delete
	rel, ok := s.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationID)
	}

	m[SchemaKey] = rel.rel.Namespace
	m[TableKey] = rel.rel.RelationName

	o, err := jsonparsing.ReadTuple(msg.OldTuple, rel.rel.Columns, s.typeMap, decode.ReadTupleConfig{
		ReadAll:  true,
		Encoding: decode.EncodingFormat{Binary: s.BinaryContentFormat},
	},
		rel.keyMap)
	if err != nil {
		return err
	}
	m[IdentityKey] = o

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnLogicalDecodingMessage is called when LogicalDecodingMessageV2 is received from replication connection
func (s *ListenerJSON) OnLogicalDecodingMessage(msg *pglogrepl.LogicalDecodingMessageV2) error {
	m := make(map[string]any)
	m[ActionKey] = Message
	m[TransactionalKey] = msg.Transactional
	m[PrefixKey] = msg.Prefix

	m[ContentKey] = decode.EncodeBinary(msg.Content, s.BinaryContentFormat)

	j, err := sonic.Marshal(m)
	if err != nil {
		return err
	}
	s.c <- j

	return nil
}

// OnTruncate is called when TruncateMessageV2 is received from replication connection
func (s *ListenerJSON) OnTruncate(msg *pglogrepl.TruncateMessageV2) error {
	// we store JSON for all relations truncated in transaction
	// this is to make sure that we either send JSON for all relations or for none (in case some marshal error occurs)
	slice := make([][]byte, len(msg.RelationIDs), len(msg.RelationIDs))
	for i := 0; i < len(msg.RelationIDs); i++ {
		m := make(map[string]any)
		m[ActionKey] = Truncate
		rel, ok := s.relations[msg.RelationIDs[i]]
		if !ok {
			return fmt.Errorf("%v: %d", errRelationNotFound, msg.RelationIDs[i])
		}
		m[SchemaKey] = rel.rel.Namespace
		m[TableKey] = rel.rel.RelationName

		j, err := sonic.Marshal(m)
		if err != nil {
			return err
		}

		slice[i] = j
	}

	for i := 0; i < len(slice); i++ {
		s.c <- slice[i]
	}
	return nil
}

// OnRelation is called when RelationMessageV2 is received from replication connection
func (s *ListenerJSON) OnRelation(msg *pglogrepl.RelationMessageV2) error {
	s.relationVersion++
	version := s.relationVersion

	keys := make([]string, 0, len(msg.Columns))
	for _, col := range msg.Columns {
		keys = append(keys, col.Name)
	}

	keyMap := keymap.New(keys...)
	s.relations[msg.RelationID] = &relationVersion{
		rel:     msg,
		version: version,
		keyMap:  keyMap,
	}

	s.arena.Add(msg.Namespace+"."+msg.RelationName, strconv.FormatUint(version, 10), keyMap)

	return nil
}

// OnType is called when TypeMessageV2 is received from replication connection
func (s *ListenerJSON) OnType(_ *pglogrepl.TypeMessageV2) error {
	// do nothing
	return nil
}

func (s *ListenerJSON) CommitPos() pglogrepl2json.CommitPoint {
	return pglogrepl2json.CommitPoint{
		LSN: s.commitPos,
		XID: s.commitXid,
	}
}

func (s *ListenerJSON) PluginArguments(pubName string) ([]string, error) {
	return []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", pubName),
		"messages 'true'",
		"streaming 'true'",
	}, nil
}

func (s *ListenerJSON) SaveState(ctx context.Context, lsn pglogrepl.LSN) error {
	return nil
}

// Close closes the listener.
func (s *ListenerJSON) Close(ctx context.Context) error {
	return nil
}
