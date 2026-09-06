package config

import "strings"

const (
	AllColumns string = "all"
)

type TableConfig struct {
	Columns []string          `json:"columns"`
	Options map[string]string `json:"options"`
}

func NewTableConfig(columns []string) *TableConfig {
	return &TableConfig{
		Columns: columns,
		Options: make(map[string]string),
	}
}

func (tc *TableConfig) AllColumns() bool {
	return len(tc.Columns) == 0 || strings.EqualFold(tc.Columns[0], AllColumns)
}

func (tc *TableConfig) Option(opt string) string {
	v, exist := tc.Options[opt]
	if !exist {
		return ""
	}
	return v
}
