package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder_BuildSelectQuery(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("full table select", func(t *testing.T) {
		query := qb.BuildSelectQuery("public.users", nil, "")
		assert.Equal(t, "SELECT * FROM public.users", query)
	})

	t.Run("where clause select", func(t *testing.T) {
		query := qb.BuildSelectQuery("public.users", []string{"id", "name"}, "active = true")
		assert.Equal(t, "SELECT id, name FROM public.users WHERE active = true", query)
	})

	t.Run("empty columns slice still selects all columns", func(t *testing.T) {
		query := qb.BuildSelectQuery("public.users", []string{}, "")
		assert.Equal(t, "SELECT * FROM public.users", query)
	})
}

func TestQueryBuilder_BuildBatchQuery(t *testing.T) {
	qb := NewQueryBuilder()

	t.Run("all columns, no where clause", func(t *testing.T) {
		query := qb.BuildBatchQuery("SELECT * FROM public.users", false)
		assert.Equal(t, "SELECT * FROM public.users WHERE ctid >= $1 AND ctid <= $2 ORDER BY ctid", query)
	})

	t.Run("specific columns, with where clause", func(t *testing.T) {
		query := qb.BuildBatchQuery("SELECT id, name FROM public.users WHERE (active = true)", true)
		assert.Equal(t, "SELECT id, name FROM public.users WHERE (active = true) AND ctid >= $1 AND ctid <= $2 ORDER BY ctid", query)
	})

	t.Run("appends ctid filter after complex query clause", func(t *testing.T) {
		query := qb.BuildBatchQuery("SELECT * FROM public.users WHERE (status = 'active' OR status = 'pending')", true)
		assert.Equal(t, "SELECT * FROM public.users WHERE (status = 'active' OR status = 'pending') AND ctid >= $1 AND ctid <= $2 ORDER BY ctid", query)
	})
}
