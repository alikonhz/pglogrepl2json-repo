package pgwal

import (
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/jackc/pglogrepl"
)

type WriterError interface {
	Error() string
	IsRetryable() bool
	IsDownStreamDown() bool
}

type Response struct {
	Entries []*ResponseEntry
	MaxLSN  pglogrepl.LSN
}

type ErrResponse struct {
	Entries []*ErrorResponseEntry
}

type ResponseEntry struct {
	PK     string
	Table  pgschema.TableName
	LSN    pglogrepl.LSN
	XID    uint32
	Offset uint32
}

type ErrorResponseEntry struct {
	ResponseEntry ResponseEntry

	Reason WriterError
}
