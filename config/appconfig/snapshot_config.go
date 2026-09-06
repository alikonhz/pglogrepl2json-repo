package appconfig

import (
	"fmt"
	"strings"
	"time"
)

type SnapshotMode string

const (
	ModeOneTime     SnapshotMode = "onetime"
	ModeOneTimeOnly SnapshotMode = "onetime_only"
	ModeAuto        SnapshotMode = "auto"
	ModeNever       SnapshotMode = "never"
)

type SnapshotTableType string

const (
	SnapshotTableFull  SnapshotTableType = "full"
	SnapshotTableQuery SnapshotTableType = "query"
)

type SnapshotTableConfig struct {
	Name  string            `json:"name"`
	Type  SnapshotTableType `json:"type"`
	Query string            `json:"query"`
}

type SnapshotConfig struct {
	Mode              SnapshotMode           `json:"mode"`
	Config            []*SnapshotTableConfig `json:"config"`
	BatchSize         int                    `json:"batchSize"`
	ParallelWorkers   int                    `json:"parallelWorkers"`
	AbortOnError      bool                   `json:"abortOnError"`
	QueryTimeout      string                 `json:"queryTimeout"`
	MaxWriteQueueSize uint64                 `json:"maxWriteQueueSize"`
}

func (c *SnapshotConfig) Validate() error {
	if c == nil {
		return nil
	}

	if c.Mode == "" {
		c.Mode = ModeOneTime
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}

	if c.ParallelWorkers <= 0 {
		c.ParallelWorkers = 1
	}

	if c.Mode != ModeNever && len(c.Config) == 0 {
		return fmt.Errorf("snapshot config must define at least one table when mode is %q", c.Mode)
	}

	if c.QueryTimeout != "" {
		if _, err := time.ParseDuration(c.QueryTimeout); err != nil {
			return fmt.Errorf("invalid snapshot query timeout %q: %w", c.QueryTimeout, err)
		}
	}

	seenTables := make(map[string]struct{}, len(c.Config))
	for _, tableCfg := range c.Config {
		if tableCfg == nil {
			return fmt.Errorf("snapshot config is empty")
		}

		tableName := strings.TrimSpace(tableCfg.Name)
		if tableName == "" {
			return fmt.Errorf("snapshot table name must not be empty")
		}

		if _, ok := seenTables[tableName]; ok {
			return fmt.Errorf("snapshot table %q is configured more than once", tableName)
		}
		seenTables[tableName] = struct{}{}
		tableCfg.Name = tableName

		switch tableCfg.Type {
		case SnapshotTableFull:
		case SnapshotTableQuery:
			if strings.TrimSpace(tableCfg.Query) == "" {
				return fmt.Errorf("snapshot query for table %q must not be empty", tableName)
			}
		default:
			return fmt.Errorf("unsupported snapshot type %q for table %q", tableCfg.Type, tableName)
		}
	}

	return nil
}
