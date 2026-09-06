package snapshot

import (
	"fmt"
	"testing"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/entrypool"
	"github.com/alikonhz/pglogrepl2json/keymap"
	"github.com/alikonhz/pglogrepl2json/orderedmap"
	"github.com/alikonhz/pglogrepl2json/pgschema"
)

var (
	benchmarkSnapshotPK   string
	benchmarkSnapshotSize uint32
)

func BenchmarkSnapshotRowBuild(b *testing.B) {
	for _, columnCount := range []int{5, 20, 100} {
		table, columns, values, rowKeyMap := benchmarkSnapshotRowData(columnCount)

		b.Run(fmt.Sprintf("new_columns_%d", columnCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tuple := orderedmap.New(rowKeyMap)
				for colIndex, column := range columns {
					tuple.Set(column, values[colIndex])
				}

				benchmarkSnapshotPK = pgschema.CreatePK(table, tuple)
				benchmarkSnapshotSize = tuple.Size()
			}
		})

		b.Run(fmt.Sprintf("pool_columns_%d", columnCount), func(b *testing.B) {
			pool := entrypool.NewPool()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tuple := pool.Get(table.FullName(), "1", rowKeyMap)
				for colIndex, column := range columns {
					tuple.Set(column, values[colIndex])
				}

				benchmarkSnapshotPK = pgschema.CreatePK(table, tuple)
				benchmarkSnapshotSize = tuple.Size()
				tuple.Release()
			}
		})
	}
}

func benchmarkSnapshotRowData(columnCount int) (*pgschema.Table, []string, []any, *keymap.KeyMap) {
	table := pgschema.NewTable("public", fmt.Sprintf("bench_%d", columnCount))
	columns := make([]string, columnCount)
	values := make([]any, columnCount)

	for i := range columns {
		columns[i] = fmt.Sprintf("col_%d", i)
		values[i] = i
		table.AddColumn(&pgschema.Column{
			Name:     columns[i],
			IsPK:     i == 0,
			Position: i + 1,
		})
	}

	return table, columns, values, snapshotKeyMap(columns, appconfig.NewTxCommitTimeOptions(""))
}
