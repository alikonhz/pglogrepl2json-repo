package snapshot

import (
	"strings"
)

type QueryBuilder struct {
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (qb *QueryBuilder) BuildSelectQuery(tableName string, columns []string, whereClause string) string {
	cols := "*"
	if len(columns) > 0 {
		cols = strings.Join(columns, ", ")
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(cols)
	sb.WriteString(" FROM ")
	sb.WriteString(tableName)

	if whereClause != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(whereClause)
	}

	return sb.String()
}

func (qb *QueryBuilder) BuildBatchQuery(baseQuery string, hasWhereClause bool) string {
	var sb strings.Builder
	sb.WriteString(baseQuery)

	if hasWhereClause {
		sb.WriteString(" AND ")
	} else {
		sb.WriteString(" WHERE ")
	}

	sb.WriteString("ctid >= $1 AND ctid <= $2")

	sb.WriteString(" ORDER BY ctid")

	return sb.String()
}
