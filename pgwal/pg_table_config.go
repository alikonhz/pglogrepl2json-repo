package pgwal

import (
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/pgschema"
)

type PGTableWithConfig struct {
	Table *pgschema.Table
	Cfg   *config.TableConfig
}

func (t *PGTableWithConfig) FullName() string {
	return t.Table.FullName()
}
