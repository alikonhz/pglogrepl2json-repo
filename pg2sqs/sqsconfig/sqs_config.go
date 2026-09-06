package sqsconfig

import (
	"strings"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
)

const (
	pg2sqsPrefix = "PG2SQS"
)

func Prefix() string {
	return pg2sqsPrefix
}

type SQSConfig struct {
	CommitTimeColumn string `json:"commitTimeColumn"`
}

type SQSAppConfig struct {
	Debug              bool                       `json:"debug"`
	Postgres           pgconfig.PgConfig          `json:"postgres"`
	SQS                SQSConfig                  `json:"sqs"`
	Tables             map[string]*SQSTableConfig `json:"tables"`
	SQSFlushInterval   string                     `json:"flushInterval"`   // ms, default is 500
	SQSFlushQueueDepth uint32                     `json:"flushQueueDepth"` // default (and max) is 32
	SQSFlushWorkers    uint32                     `json:"flushWorkers"`    // default is runtime.GOMAXPROCS
	SQSWriteTimeout    string                     `json:"writeTimeout"`
	StatsInterval      string                     `json:"statsInterval"`
	SQSRetryPolicy     appconfig.RetryPolicy      `json:"retryPolicy"`
	MaxWriteQueueSize  uint64                     `json:"maxWriteQueueSize"`
	ShutdownTimeout    string                     `json:"shutdownTimeout"`
}

type SQSTableConfig struct {
	Columns     []string          `json:"columns"`
	Options     map[string]string `json:"options"`
	QueueConfig *SQSQueueConfig   `json:"queue"`
}

type SQSQueueConfig struct {
	Name    string          `json:"name"`
	URL     string          `json:"url"` // URL is not updatable in config
	Insert  *SQSQueueConfig `json:"insert"`
	Update  *SQSQueueConfig `json:"update"`
	Delete  *SQSQueueConfig `json:"delete"`
	GroupID string          `json:"groupID"` //nolint
}

var (
	_ = appconfig.AppConfig((*SQSAppConfig)(nil))
)

func (cfg *SQSAppConfig) PostgresConfig() pgconfig.PgConfig {
	return cfg.Postgres
}

func (cfg *SQSAppConfig) SnapshotConfig() *appconfig.SnapshotConfig {
	// no snapshots for SQS
	return nil
}

func (cfg *SQSAppConfig) Settings() appconfig.AppSettings {
	return appconfig.AppSettings{
		StatsInterval:     cfg.StatsInterval,
		MaxWriteQueueSize: cfg.MaxWriteQueueSize,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	}
}

func (cfg *SQSAppConfig) StatsIntervalConfig() string {
	return cfg.StatsInterval
}

func (cfg *SQSAppConfig) MaxWriteQueueSizeConfig() uint64 {
	return cfg.MaxWriteQueueSize
}

func (cfg *SQSAppConfig) TablesConfig() map[string]*config.TableConfig {
	m := make(map[string]*config.TableConfig)
	for key, tableConfig := range cfg.Tables {
		m[key] = &config.TableConfig{
			Columns: tableConfig.Columns,
			Options: tableConfig.Options,
		}
	}

	return m
}

func (cfg *SQSAppConfig) NeedsTrackCommitTimestamp() bool {
	return cfg.SQS.CommitTimeColumn != ""
}

func (cfg *SQSAppConfig) RetryPolicy() appconfig.RetryPolicy {
	return cfg.SQSRetryPolicy
}

func LoadAppConfigNoEnv(configPath ...string) (*SQSAppConfig, error) {
	builder := &SQSAppConfigBuilder{} //nolint

	err := appconfig.LoadAppConfigNoEnv(builder, configPath...)

	if err != nil {
		return nil, err //nolint
	}

	return builder.config, nil
}

func LoadAppConfig(configPath ...string) (*SQSAppConfig, error) {
	builder := &SQSAppConfigBuilder{} //nolint
	err := appconfig.LoadAppConfig(builder, configPath...)

	if err != nil {
		return nil, err //nolint
	}

	return builder.config, nil
}

type SQSAppConfigBuilder struct {
	config *SQSAppConfig
}

func (b *SQSAppConfigBuilder) CreateEmptyConfig() any {
	b.config = &SQSAppConfig{} //nolint

	return b.config
}

func (b *SQSAppConfigBuilder) Prefix() string {
	return pg2sqsPrefix
}

func (b *SQSAppConfigBuilder) WellForm(env any) {
	if b.config == nil {
		return
	}

	if envMap, ok := env.(map[string]any); ok {
		b.mergeFromEnv(envMap)
	}

	b.config.Tables = wellFormTableNames(b.config.Tables)
	b.config.Postgres = appconfig.WellFormPgConfig(b.config.Postgres)
}

func (b *SQSAppConfigBuilder) Validate() error {
	return nil
}

func (b *SQSAppConfigBuilder) mergeFromEnv(envMap map[string]any) {
	if b.config == nil {
		b.CreateEmptyConfig()
	}

	if b.config.Tables == nil {
		b.config.Tables = make(map[string]*SQSTableConfig)
	}

	for _, value := range envMap {
		var (
			tableMap map[string]any
			ok       bool
		)

		if tableMap, ok = value.(map[string]any); !ok {
			continue
		}

		name, cfg := readTableCfgFromEnv(tableMap)
		if name != "" {
			b.config.Tables[name] = cfg
		}
	}
}

func readTableCfgFromEnv(tableMap map[string]any) (string, *SQSTableConfig) {
	var tableName string

	if name, ok := tableMap["name"]; ok {
		if str, ok := name.(string); ok {
			tableName = str
		}
	}

	if tableName == "" {
		return "", nil
	}

	columns := readSlice(tableMap["columns"])
	if columns == nil {
		return "", nil
	}

	q := readQueueOpts(tableMap["q"])
	if q == nil {
		return "", nil
	}

	return tableName, &SQSTableConfig{
		Columns:     columns,
		Options:     make(map[string]string),
		QueueConfig: q,
	}
}

func readQueueOpts(q any) *SQSQueueConfig {
	var (
		queueMap map[string]any
		ok       bool
	)

	if queueMap, ok = q.(map[string]any); !ok {
		return nil
	}

	queueConf := readQueueConfig(queueMap)
	if queueConf == nil {
		queueConf = &SQSQueueConfig{}
	}

	ins := readQueueConfig(queueMap["insert"])
	upd := readQueueConfig(queueMap["update"])
	del := readQueueConfig(queueMap["delete"])

	queueConf.Insert = ins
	queueConf.Update = upd
	queueConf.Delete = del

	return queueConf
}

func readQueueConfig(q any) *SQSQueueConfig {
	var (
		m  map[string]any
		ok bool
	)

	if m, ok = q.(map[string]any); !ok {
		return nil
	}

	var (
		name    string
		groupID string
	)

	if n, ok := m["name"]; ok {
		if ns, ok := n.(string); ok {
			name = ns
		}
	}

	if g, ok := m["groupid"]; ok {
		if gs, ok := g.(string); ok {
			groupID = gs
		}
	}

	return &SQSQueueConfig{
		Name:    name,
		GroupID: groupID,
	}
}

func readSlice(slice any) []string {
	var (
		str string
		ok  bool
	)

	if str, ok = slice.(string); !ok {
		return nil
	}

	parts := strings.Split(str, ",")

	trimmed := make([]string, len(parts))

	for i := 0; i < len(parts); i++ {
		trimmed[i] = strings.TrimSpace(parts[i])
	}

	return trimmed
}

func wellFormTableNames(tablesConfig map[string]*SQSTableConfig) map[string]*SQSTableConfig {
	m := make(map[string]*SQSTableConfig)

	for key, sqsTableConfig := range tablesConfig {
		newKey := appconfig.WellFormTableName(key)

		if sqsTableConfig.Options == nil {
			sqsTableConfig.Options = make(map[string]string)
		}

		m[newKey] = sqsTableConfig
	}

	return m
}
