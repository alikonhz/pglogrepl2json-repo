package pgwal

import (
	"iter"
	"time"

	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/jackc/pglogrepl"
)

type OpKind uint8

const (
	Insert OpKind = 0
	Update OpKind = 1
	Delete OpKind = 2

	SnapshotRead OpKind = 3
)

type Table struct {
	Schema   string
	Table    string
	FullName string
}

type EntryTuple interface {
	Get(string) (any, bool)
	GetValue(string) any

	Set(string, any)
	Iter() iter.Seq2[string, any]

	MarshalJSON() ([]byte, error)
	CustomMarshalJSON(keys []string, custom []orderedmap.ValuePair) ([]byte, error)

	Size() uint32

	Release()
}

type WriteEntry struct {
	Tuple     EntryTuple
	PrevTuple EntryTuple
	PK        string
	Kind      OpKind
	Table     pgschema.TableName
	Offset    uint32
}

func (entry *WriteEntry) Release() {
	if entry.Tuple != nil {
		entry.Tuple.Release()
	}

	if entry.PrevTuple != nil {
		entry.PrevTuple.Release()
	}
}

type WriteRequest struct {
	Entries    []*WriteEntry
	LSN        pglogrepl.LSN
	XID        uint32
	CommitTime time.Time

	isSnapshot bool
}

func NewSnapshotRequest(lsn pglogrepl.LSN, entries []*WriteEntry) *WriteRequest {
	return &WriteRequest{
		Entries:    entries,
		LSN:        lsn,
		XID:        0,
		CommitTime: time.Time{},
		isSnapshot: true,
	}
}

func (r *WriteRequest) IsSnapshot() bool {
	return r.isSnapshot
}

func (r *WriteRequest) Size() uint32 {
	return uint32(len(r.Entries))
}

func (r *WriteRequest) GetEntryByOffset(offset uint32) *WriteEntry {
	if len(r.Entries) == 0 {
		return nil
	}

	for _, entry := range r.Entries {
		if entry.Offset == offset {
			return entry
		}
	}

	return nil
}
