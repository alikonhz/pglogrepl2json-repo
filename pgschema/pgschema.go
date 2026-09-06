package pgschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

var (
	ErrPubNotFound           = errors.New("publication not found")
	ErrPGVersionNotSupported = errors.New("PG version not supported")
)

type PGVersion uint8

const (
	Unknown PGVersion = 0
	V12     PGVersion = 12
	V13     PGVersion = 13 // logical_decoding_work_mem
	V14     PGVersion = 14 // protocol_version = 2
	V15     PGVersion = 15 // publications with custom columns
	V16     PGVersion = 16
	V17     PGVersion = 17
	V18     PGVersion = 18
)

type Table struct {
	Name TableName

	Columns              map[string]*Column
	PK                   map[string]*Column
	PhysicalColumnsCount int
}

type TableName struct {
	Schema   string
	Name     string
	FullName string
}

func (tn TableName) String() string {
	return tn.FullName
}

func NewTable(schema, name string) *Table {
	return &Table{
		Name:    MakeTableName(schema, name),
		Columns: make(map[string]*Column),
		PK:      make(map[string]*Column),
	}
}

type Column struct {
	Name     string
	TypeID   uint32
	IsPK     bool
	Position int
}

func (t *Table) HasColumn(col string) bool {
	_, ok := t.Columns[col]
	return ok
}

func (t *Table) FullName() string {
	return t.Name.FullName
}

func (t *Table) AddColumn(col *Column) {
	t.Columns[col.Name] = col
	if col.IsPK {
		t.PK[col.Name] = col
	}
}

func (t *Table) WithColumns(cols map[string]*Column) *Table {
	return &Table{
		Name:                 t.Name,
		Columns:              cols,
		PK:                   t.PK,
		PhysicalColumnsCount: t.PhysicalColumnsCount,
	}
}

func (t *Table) HasAllPhysicalColumns() bool {
	return t != nil && t.PhysicalColumnsCount > 0 && len(t.Columns) == t.PhysicalColumnsCount
}

func (t *Table) MakeKey(pk string) string {
	return MakeRowKey(t.Name.FullName, pk)
}

type TupleReader interface {
	Get(string) (any, bool)
}

func CreatePK(pgTable *Table, tr TupleReader) string {
	key := ""
	colon := ""

	pkColumns := make([]*Column, 0, len(pgTable.PK))
	for _, col := range pgTable.PK {
		pkColumns = append(pkColumns, col)
	}

	slices.SortFunc(pkColumns, func(a, b *Column) int {
		if a.Position < b.Position {
			return -1
		}
		if a.Position > b.Position {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})

	for _, col := range pkColumns {
		val, _ := tr.Get(col.Name)
		key += colon + fmt.Sprintf("%v", val)
		colon = ":"
	}

	return key
}

func MakeRowKey(tableKey, pk string) string {
	return fmt.Sprintf("%s:%s", tableKey, pk)
}

func MakeTableName(schema, table string) TableName {
	fullName := fmt.Sprintf("%s.%s", schema, table)

	return TableName{
		Schema:   schema,
		Name:     table,
		FullName: fullName,
	}
}

func ParseTableName(fullName string) TableName {
	parts := strings.Split(fullName, ".")
	if len(parts) == 2 {
		return MakeTableName(parts[0], parts[1])
	}
	return MakeTableName("public", fullName)
}

func ReadPubTablesV12(ctx context.Context, conn *pgx.Conn, publication string) (map[string]*Table, error) {
	err := publicationExists(ctx, conn, publication)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(ctx,
		`
SELECT DISTINCT table_schema, table_name, column_name, pga.atttypid, coalesce(i.indisprimary, false) as indisprimary, ordinal_position 
FROM pg_publication_tables ppt
LEFT JOIN information_schema.columns c on ppt.schemaname = table_schema and ppt.tablename = table_name
LEFT JOIN pg_namespace pgn on c.table_schema = pgn.nspname
LEFT JOIN pg_class pgc on pgc.relnamespace = pgn.oid and c.table_name = pgc.relname
LEFT JOIN pg_attribute pga on pga.attrelid = pgc.oid and pga.attname = c.column_name
LEFT JOIN pg_index i on pga.attrelid = i.indrelid AND pga.attnum = ANY(i.indkey) AND i.indisprimary
WHERE ppt.pubname = $1
ORDER BY table_schema, table_name, ordinal_position`, publication)

	if err != nil {
		return nil, err
	}

	tables, err := pgx.CollectRows[*pubTableColumn](rows, func(row pgx.CollectableRow) (*pubTableColumn, error) {
		var (
			schema, table, column string
			typeID                uint32
			isPK                  bool
			position              int
		)
		err := row.Scan(&schema, &table, &column, &typeID, &isPK, &position)
		if err != nil {
			return nil, err
		}

		return &pubTableColumn{
			Schema:   schema,
			Table:    table,
			Column:   column,
			TypeID:   typeID,
			IsPK:     isPK,
			Position: position,
		}, nil
	})

	if err != nil {
		return map[string]*Table{}, err
	}

	res := make(map[string]*Table)
	for _, col := range tables {
		key := MakeTableName(col.Schema, col.Table)
		table, exists := res[key.FullName]
		if !exists {
			table = NewTable(col.Schema, col.Table)
			res[key.FullName] = table
		}

		// in PG versions >= 12 and <= 14 all columns are added to the publication
		table.AddColumn(&Column{
			Name:     col.Column,
			TypeID:   col.TypeID,
			IsPK:     col.IsPK,
			Position: col.Position,
		})
	}

	for _, table := range res {
		table.PhysicalColumnsCount = len(table.Columns)
	}

	return res, nil
}

func ReadPGVersion(ctx context.Context, conn *pgx.Conn) (PGVersion, error) {
	row := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int")
	var versionNum int
	err := row.Scan(&versionNum)
	if err != nil {
		return Unknown, fmt.Errorf("pgschema: error reading server version: %w", err)
	}

	return parsePGVersionNum(versionNum)
}

func parsePGVersionNum(versionNum int) (PGVersion, error) {
	ver := versionNum / 10000
	if ver < 12 {
		return Unknown, fmt.Errorf("%w: %d", ErrPGVersionNotSupported, versionNum)
	}

	return PGVersion(ver), nil
}

type PubTableRes struct {
	Tables               map[string]*Table
	TablesWithAllColumns map[string]*Table
}

// ReadPubTables reads tables for provided publication. This function first checks underlying PG version and then
// calls the appropriate function depending on the version.
func ReadPubTables(ctx context.Context, conn *pgx.Conn, publication string, ver PGVersion) (PubTableRes, error) {
	if ver >= V12 && ver < V15 {
		pubTables, err := ReadPubTablesV12(ctx, conn, publication)
		if err != nil {
			return PubTableRes{}, err
		}
		return PubTableRes{
			Tables:               pubTables,
			TablesWithAllColumns: pubTables,
		}, nil
	}

	if ver >= V15 {
		pubTables, pubTablesWithAllColumns, err := ReadPubTablesV15(ctx, conn, publication)
		if err != nil {
			return PubTableRes{}, err
		}

		return PubTableRes{
			Tables:               pubTables,
			TablesWithAllColumns: pubTablesWithAllColumns,
		}, nil
	}

	return PubTableRes{}, fmt.Errorf("unsupported PG version: %d", ver)
}

// ReadPubTablesV15 reads tables for provided publication. Works only on PG version >= 15
// if publication doesn't exist - error ErrPubNotFound is returned
func ReadPubTablesV15(ctx context.Context, conn *pgx.Conn, publication string) (map[string]*Table, map[string]*Table, error) {
	err := publicationExists(ctx, conn, publication)
	if err != nil {
		return nil, nil, err
	}

	rows, err := conn.Query(ctx,
		`
SELECT DISTINCT ppt.attnames, table_schema, table_name, column_name, pga.atttypid, coalesce(i.indisprimary, false) as indisprimary, ordinal_position 
FROM pg_publication_tables ppt
LEFT JOIN information_schema.columns c on ppt.schemaname = table_schema and ppt.tablename = table_name
LEFT JOIN pg_namespace pgn on c.table_schema = pgn.nspname
LEFT JOIN pg_class pgc on pgc.relnamespace = pgn.oid and c.table_name = pgc.relname
LEFT JOIN pg_attribute pga on pga.attrelid = pgc.oid and pga.attname = c.column_name
LEFT JOIN pg_index i on pga.attrelid = i.indrelid AND pga.attnum = ANY(i.indkey) AND i.indisprimary
WHERE ppt.pubname = $1
ORDER BY table_schema, table_name, ordinal_position`, publication)
	tables, err := pgx.CollectRows[*pubTableColumn](rows, func(row pgx.CollectableRow) (*pubTableColumn, error) {
		var (
			names                 []string
			schema, table, column *string
			typeID                *uint32
			isPK                  *bool
			position              *int
		)

		err := row.Scan(&names, &schema, &table, &column, &typeID, &isPK, &position)

		if err != nil {
			return nil, err
		}

		if schema == nil || table == nil || column == nil {
			return nil, fmt.Errorf("unable to read table data. make sure user has SELECT permissions on all tables from configuration")
		}

		return &pubTableColumn{
			colNames: names,
			Schema:   *schema,
			Table:    *table,
			Column:   *column,
			TypeID:   *typeID,
			IsPK:     *isPK,
			Position: *position,
		}, nil
	})

	if err != nil {
		return map[string]*Table{}, map[string]*Table{}, err
	}

	pubTables := make(map[string]*Table)
	pubTablesWithAllColumns := make(map[string]*Table)

	for _, col := range tables {
		key := MakeTableName(col.Schema, col.Table)
		table1, ok := pubTables[key.FullName]
		if !ok {
			table1 = NewTable(col.Schema, col.Table)
			pubTables[key.FullName] = table1
		}
		table2, ok := pubTablesWithAllColumns[key.FullName]
		if !ok {
			table2 = NewTable(col.Schema, col.Table)
			pubTablesWithAllColumns[key.FullName] = table2
		}

		colExistsInPub := col.colNames == nil || slices.ContainsFunc(col.colNames, func(s string) bool {
			return strings.EqualFold(s, col.Column)
		})
		column := &Column{
			Name:     col.Column,
			TypeID:   col.TypeID,
			IsPK:     col.IsPK,
			Position: col.Position,
		}

		table2.AddColumn(column)
		if colExistsInPub {
			table1.AddColumn(column)
		}
	}

	for name, table := range pubTablesWithAllColumns {
		table.PhysicalColumnsCount = len(table.Columns)
		pubTables[name].PhysicalColumnsCount = table.PhysicalColumnsCount
	}

	return pubTables, pubTablesWithAllColumns, nil
}

type pubTableColumn struct {
	colNames []string
	Schema   string
	Table    string
	Column   string
	TypeID   uint32
	IsPK     bool
	Position int
}

func publicationExists(ctx context.Context, conn *pgx.Conn, publication string) error {
	r := conn.QueryRow(ctx, "SELECT pubname FROM pg_publication WHERE pubname = $1", publication)
	var pubName string
	err := r.Scan(&pubName)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrPubNotFound, publication)
	}

	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	return nil
}
