package redisconfig

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

const (
	pg2redisPrefix = "PG2REDIS"
)

func Prefix() string {
	return pg2redisPrefix
}

type RedisConfig struct {
	Conn             config.ConnectionOpts `json:"conn"`
	CommitTimeColumn string                `json:"commitTimeColumn"`
}

type OperationType string

const (
	OpInsert OperationType = "insert"
	OpUpdate OperationType = "update"
	OpDelete OperationType = "delete"
)

// ConditionOperator defines supported comparison operators
type ConditionOperator string

const (
	OpEqual          ConditionOperator = "="
	OpNotEqual       ConditionOperator = "<>"
	OpLess           ConditionOperator = "<"
	OpGreater        ConditionOperator = ">"
	OpLessEq         ConditionOperator = "<="
	OpGreaterEq      ConditionOperator = ">="
	OpIn             ConditionOperator = "in"
	OpNotIn          ConditionOperator = "not_in"
	OpIsNull         ConditionOperator = "is_null"
	OpIsNotNull      ConditionOperator = "is_not_null"
	OpIsDistinctFrom ConditionOperator = "is_distinct_from"
)

// ConditionConfig defines the condition for command execution
type ConditionConfig struct {
	Op     ConditionOperator `json:"op"`
	Column string            `json:"column"`
	Value  string            `json:"value,omitempty"`  // For single-value operators
	Values []string          `json:"values,omitempty"` // For "in", "not_in" operators
}

// RedisConditionalCommandConfig represents a command or command group with an optional condition
type RedisConditionalCommandConfig struct {
	// Command can be either []string or [][]string
	Command   any              `json:"command"`
	Condition *ConditionConfig `json:"condition,omitempty"`
}

// RedisCommandWithCondition is a union type that accepts both:
// - []string (legacy format - always execute)
// - RedisConditionalCommandConfig (new format with optional condition)
type RedisCommandWithCondition struct {
	Commands  [][]string
	Condition *ConditionConfig
}

func (r *RedisCommandWithCondition) UnmarshalJSON(b []byte) error {
	// Try unmarshaling as [][]string first (multi-command)
	var multi [][]string
	if err := json.Unmarshal(b, &multi); err == nil {
		r.Commands = multi
		r.Condition = nil
		return nil
	}

	// Try unmarshaling as []string next (legacy format)
	var legacy []string
	if err := json.Unmarshal(b, &legacy); err == nil {
		r.Commands = [][]string{legacy}
		r.Condition = nil
		return nil
	}

	// Try unmarshaling as RedisConditionalCommandConfig
	var conditional struct {
		Command   any              `json:"command"`
		Condition *ConditionConfig `json:"condition,omitempty"`
	}

	if err := json.Unmarshal(b, &conditional); err == nil && conditional.Command != nil {
		cmds, err := parseCommands(conditional.Command)
		if err != nil {
			return err
		}
		r.Commands = cmds
		r.Condition = conditional.Condition
		return nil
	}

	return fmt.Errorf("failed to unmarshal Redis command: must be either string slice, list of string slices, or conditional command object")
}

type RedisCommandConfig []string

type TableOperationConfig struct {
	Commands []any `json:"commands"`
}

type RedisTableConfig struct {
	FullName string                `json:"-"`
	Options  map[string]string     `json:"options"`
	Insert   *TableOperationConfig `json:"insert,omitzero"`
	Update   *TableOperationConfig `json:"update,omitzero"`
	Delete   *TableOperationConfig `json:"delete,omitzero"`
	Commands []any                 `json:"commands,omitzero"` // Shortcut: same commands for all ops

	// Actual commands after WellForm
	ResolvedCommands []RedisCommandWithCondition `json:"-"`
	InsertCommands   []RedisCommandWithCondition `json:"-"`
	UpdateCommands   []RedisCommandWithCondition `json:"-"`
	DeleteCommands   []RedisCommandWithCondition `json:"-"`
}

func (c RedisTableConfig) Option(name string) string {
	return c.Options[name]
}

type RedisAppConfig struct {
	Debug                bool                         `json:"debug"`
	Postgres             pgconfig.PgConfig            `json:"postgres"`
	Tables               map[string]*RedisTableConfig `json:"tables"`
	Redis                RedisConfig                  `json:"redis"`
	RedisFlushInterval   string                       `json:"flushInterval"`   // ms, default is 500
	RedisFlushBufferSize uint32                       `json:"flushBufferSize"` // default is 100
	RedisFlushQueueDepth uint32                       `json:"flushQueueDepth"` // default (and max) is 32
	RedisFlushWorkers    uint32                       `json:"flushWorkers"`    // default is runtime.GOMAXPROCS
	RedisWriteTimeout    string                       `json:"writeTimeout"`
	StatsInterval        string                       `json:"statsInterval"`
	RedisRetryPolicy     appconfig.RetryPolicy        `json:"retryPolicy"`
	MaxWriteQueueSize    uint64                       `json:"maxWriteQueueSize"`
	Snapshot             *appconfig.SnapshotConfig    `json:"snapshot"`

	ShutdownTimeout string `json:"shutdownTimeout"`
}

const (
	ZIncrByKeyOptName  = "zincrby-key"
	ZIncrByIncrOptName = "zincrby-incr"
)

func LoadAppConfig(configPath ...string) (*RedisAppConfig, error) {
	builder := &RedisAppConfigBuilder{}
	err := appconfig.LoadAppConfig(builder, configPath...)
	if err != nil {
		return nil, err
	}

	return builder.config, nil
}

func LoadAppConfigNoEnv(configPath ...string) (*RedisAppConfig, error) {
	builder := &RedisAppConfigBuilder{}
	err := appconfig.LoadAppConfigNoEnv(builder, configPath...)
	if err != nil {
		return nil, err
	}

	return builder.config, nil
}

type RedisAppConfigBuilder struct {
	config        *RedisAppConfig
	validationErr error
}

func (rb *RedisAppConfigBuilder) CreateEmptyConfig() any {
	rb.config = &RedisAppConfig{}
	rb.validationErr = nil
	return rb.config
}

func NewFromConfig(cfg *RedisAppConfig) *RedisAppConfigBuilder {
	b := &RedisAppConfigBuilder{
		config: cfg,
	}

	return b
}

func (rb *RedisAppConfigBuilder) Prefix() string {
	return pg2redisPrefix
}

func (rb *RedisAppConfigBuilder) WellForm(env any) {
	if rb.config == nil {
		return
	}
	rb.validationErr = nil

	if envMap, ok := env.(map[string]any); ok {
		rb.mergeFromEnv(envMap)
	}

	// Koanf fails to unmarshal []RedisCommandWithCondition correctly when it's a mix of slices and maps.
	// We've changed it back to []any in RedisTableConfig temporarily (if we did)
	// or we need to handle it here.
	// Actually, let's just fix the unmarshaling by ensuring TableOperationConfig uses []any
	// and we convert it here.

	rb.config.Tables = wellFormTableNames(rb.config.Tables)
	rb.config.Postgres = appconfig.WellFormPgConfig(rb.config.Postgres)

	for tableName, tableConfig := range rb.config.Tables {
		var err error
		tableConfig.ResolvedCommands, err = resolveCommandsStrict(tableConfig.Commands)
		rb.addValidationErr(tableName, "commands", err)
		if tableConfig.Insert != nil {
			tableConfig.InsertCommands, err = resolveCommandsStrict(tableConfig.Insert.Commands)
			rb.addValidationErr(tableName, "insert.commands", err)
		}
		if tableConfig.Update != nil {
			tableConfig.UpdateCommands, err = resolveCommandsStrict(tableConfig.Update.Commands)
			rb.addValidationErr(tableName, "update.commands", err)
		}
		if tableConfig.Delete != nil {
			tableConfig.DeleteCommands, err = resolveCommandsStrict(tableConfig.Delete.Commands)
			rb.addValidationErr(tableName, "delete.commands", err)
		}

		if len(tableConfig.ResolvedCommands) == 0 &&
			len(tableConfig.InsertCommands) == 0 &&
			len(tableConfig.UpdateCommands) == 0 &&
			len(tableConfig.DeleteCommands) == 0 {
			// Default to HSET if no commands are defined
			tableConfig.ResolvedCommands = []RedisCommandWithCondition{{Commands: [][]string{{"HSET", fmt.Sprintf("%s:%s", tableConfig.FullName, "{%pk%}"), "{pairs:*}"}}}}
		}
	}
}

func resolveCommands(cmds []any) []RedisCommandWithCondition {
	res, _ := resolveCommandsStrict(cmds)
	return res
}

func resolveCommandsStrict(cmds []any) ([]RedisCommandWithCondition, error) {
	res := make([]RedisCommandWithCondition, 0, len(cmds))
	var err error
	for _, cmd := range cmds {
		if m, ok := cmd.(map[string]any); ok && (m["command"] != nil || m["condition"] != nil) {
			var condCmd RedisCommandWithCondition
			cmdPartsRaw, ok := m["command"]
			if !ok || cmdPartsRaw == nil {
				err = errors.Join(err, fmt.Errorf("%w: conditional command missing command", ErrInvalidCommandFormat))
				continue
			}
			{
				cmds, parseErr := parseCommands(cmdPartsRaw)
				if parseErr != nil {
					err = errors.Join(err, fmt.Errorf("conditional command: %w", parseErr))
					continue
				}
				condCmd.Commands = cmds
			}
			if condMap, ok := m["condition"].(map[string]any); ok {
				cond := &ConditionConfig{}
				if op, ok := condMap["op"].(string); ok {
					cond.Op = ConditionOperator(op)
				}
				if col, ok := condMap["column"].(string); ok {
					cond.Column = col
				}
				if val, ok := condMap["value"].(string); ok {
					cond.Value = val
				}
				if valsRaw, ok := condMap["values"]; ok {
					cond.Values = resolveStringSlice(valsRaw)
				}
				condCmd.Condition = cond
			}
			if len(condCmd.Commands) > 0 {
				res = append(res, condCmd)
			}
			continue
		}

		// Otherwise try to parse as single or multi-command
		parsed, parseErr := parseCommands(cmd)
		if parseErr != nil {
			err = errors.Join(err, fmt.Errorf("command: %w", parseErr))
			continue
		}
		if len(parsed) > 0 {
			res = append(res, RedisCommandWithCondition{Commands: parsed})
		}
	}
	return res, err
}

func (rb *RedisAppConfigBuilder) addValidationErr(tableName, field string, err error) {
	if err == nil {
		return
	}
	rb.validationErr = errors.Join(rb.validationErr, fmt.Errorf("table %s %s: %w", tableName, field, err))
}

var (
	ErrInvalidCommandFormat = errors.New("invalid command format")
	ErrEmptyCommandGroup    = errors.New("command group cannot be empty")
)

// parseCommands parses the command field which can be either
// a single command ([]string) or multiple commands ([][]string)
func parseCommands(raw any) ([][]string, error) {
	if raw == nil {
		return nil, ErrInvalidCommandFormat
	}

	if multi, ok := raw.([]any); ok {
		if len(multi) == 0 {
			return nil, ErrEmptyCommandGroup
		}

		// If first element is a string, it's a single command
		if _, ok := multi[0].(string); ok {
			ss, err := parseCommandStringSlice(multi, "command")
			if err != nil {
				return nil, err
			}
			return [][]string{ss}, nil
		}

		// Otherwise it's (hopefully) a list of commands
		result := make([][]string, 0, len(multi))
		for i, cmd := range multi {
			ss, parseErr := parseCommandStringSlice(cmd, fmt.Sprintf("command %d", i))
			if parseErr == ErrInvalidCommandFormat {
				return nil, fmt.Errorf("%w: command %d is invalid or empty", ErrInvalidCommandFormat, i)
			}
			if parseErr != nil {
				return nil, parseErr
			}
			result = append(result, ss)
		}
		return result, nil
	}

	// Fallback for single command if it's already a []string or map (from env)
	if ss, err := parseCommandStringSlice(raw, "command"); err == nil {
		return [][]string{ss}, nil
	} else if err != ErrInvalidCommandFormat {
		return nil, err
	}

	return nil, ErrInvalidCommandFormat
}

func parseCommandStringSlice(raw any, label string) ([]string, error) {
	parts, ok := resolveSliceParts(raw)
	if !ok || len(parts) == 0 {
		return nil, ErrInvalidCommandFormat
	}

	res := make([]string, 0, len(parts))
	for i, p := range parts {
		s, ok := p.(string)
		if !ok {
			return nil, fmt.Errorf("%w: %s argument %d is not a string", ErrInvalidCommandFormat, label, i)
		}
		res = append(res, s)
	}
	return res, nil
}

func resolveSliceParts(raw any) ([]any, bool) {
	switch v := raw.(type) {
	case []any:
		return v, true
	case []string:
		parts := make([]any, len(v))
		for i, s := range v {
			parts[i] = s
		}
		return parts, true
	case map[string]any:
		// Handle numeric-keyed maps from koanf env provider
		parts := make([]any, 0, len(v))
		for i := 0; ; i++ {
			if val, ok := v[fmt.Sprintf("%d", i)]; ok {
				parts = append(parts, val)
			} else {
				break
			}
		}
		return parts, true
	default:
		return nil, false
	}
}

func resolveStringSlice(raw any) []string {
	parts, ok := resolveSliceParts(raw)
	if !ok {
		return nil
	}

	res := make([]string, 0, len(parts))
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			res = append(res, v)
		default:
			res = append(res, fmt.Sprintf("%v", v))
		}
	}
	return res
}

func wellFormTableNames(tablesConfig map[string]*RedisTableConfig) map[string]*RedisTableConfig {
	m := make(map[string]*RedisTableConfig)

	for key, tableConfig := range tablesConfig {
		newKey := appconfig.WellFormTableName(key)
		tableConfig.FullName = newKey

		if tableConfig.Options == nil {
			tableConfig.Options = make(map[string]string)
		}

		m[newKey] = tableConfig
	}

	return m
}

func (rb *RedisAppConfigBuilder) mergeFromEnv(envMap map[string]any) {
	if rb.config == nil {
		rb.CreateEmptyConfig()
	}

	if rb.config.Tables == nil {
		rb.config.Tables = make(map[string]*RedisTableConfig)
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
			rb.config.Tables[name] = cfg
		}
	}
}

func readTableCfgFromEnv(tableMap map[string]any) (string, *RedisTableConfig) {
	var tableName string

	if name, ok := tableMap["name"]; ok {
		if str, ok := name.(string); ok {
			tableName = str
		}
	}

	if tableName == "" {
		return "", nil
	}

	options := make(map[string]string)
	if opts, ok := tableMap["options"].(map[string]any); ok {
		for k, v := range opts {
			if s, ok := v.(string); ok {
				options[k] = s
			}
		}
	}

	return tableName, &RedisTableConfig{
		Options:  options,
		Commands: readCommandsAsAnyFromEnv(tableMap["commands"]),
		Insert:   readOperationConfigFromEnv(tableMap["insert"]),
		Update:   readOperationConfigFromEnv(tableMap["update"]),
		Delete:   readOperationConfigFromEnv(tableMap["delete"]),
	}
}

func readOperationConfigFromEnv(op any) *TableOperationConfig {
	if opMap, ok := op.(map[string]any); ok {
		return &TableOperationConfig{
			Commands: readCommandsAsAnyFromEnv(opMap["commands"]),
		}
	}
	return nil
}

func readCommandsAsAnyFromEnv(commands any) []any {
	if cmdSlice, ok := commands.(map[string]any); ok {
		res := make([]any, 0, len(cmdSlice))
		for i := 0; ; i++ {
			if cmd, ok := cmdSlice[fmt.Sprintf("%d", i)]; ok {
				if parts := readSliceFromEnv(cmd); len(parts) > 0 {
					res = append(res, parts)
				} else if cmdMap, ok := cmd.(map[string]any); ok {
					res = append(res, cmdMap)
				}
			} else {
				break
			}
		}
		return res
	}
	return nil
}

func readSliceFromEnv(slice any) []string {
	var (
		str string
		ok  bool
	)

	if str, ok = slice.(string); !ok {
		return nil
	}

	parts := strings.Split(str, "/")

	trimmed := make([]string, len(parts))

	for i := 0; i < len(parts); i++ {
		trimmed[i] = strings.TrimSpace(parts[i])
	}

	return trimmed
}

func (rb *RedisAppConfigBuilder) Validate() error {
	if rb.config == nil {
		return errors.New("config is nil")
	}

	appConf := rb.config

	var err error
	err = errors.Join(err, rb.validationErr)

	for tableName, tableConfig := range appConf.Tables {
		if !strings.Contains(tableName, ".") {
			err = errors.Join(err, fmt.Errorf("table %s must follow Postgres naming conventions (schema.table)", tableName))
		}

		if len(tableConfig.ResolvedCommands) == 0 &&
			len(tableConfig.InsertCommands) == 0 &&
			len(tableConfig.UpdateCommands) == 0 &&
			len(tableConfig.DeleteCommands) == 0 {
			err = errors.Join(err, fmt.Errorf("table %s must have at least one of commands, insert, update, or delete defined", tableName))
		}

		validateCommands := func(commands []RedisCommandWithCondition) {
			for _, cmdWithCond := range commands {
				if len(cmdWithCond.Commands) == 0 {
					err = errors.Join(err, fmt.Errorf("table %s: command group is empty", tableName))
					continue
				}

				for _, cmd := range cmdWithCond.Commands {
					if len(cmd) == 0 {
						err = errors.Join(err, fmt.Errorf("table %s: empty command found", tableName))
						continue
					}
					// Basic validation: first element is command name
					cmdName := strings.ToUpper(cmd[0])
					if !isSupportedRedisCommand(cmdName) {
						err = errors.Join(err, fmt.Errorf("table %s: unsupported Redis command %s", tableName, cmdName))
					}
					if len(cmd) < 2 {
						err = errors.Join(err, fmt.Errorf("table %s: command %s must have at least a key pattern", tableName, cmdName))
					}
				}

				// Validate condition
				if cmdWithCond.Condition != nil {
					cond := cmdWithCond.Condition
					if cond.Column == "" {
						err = errors.Join(err, fmt.Errorf("table %s: condition must have a column", tableName))
					}
					switch cond.Op {
					case OpEqual, OpNotEqual, OpLess, OpGreater, OpLessEq, OpGreaterEq, OpIsDistinctFrom:
						if cond.Value == "" {
							err = errors.Join(err, fmt.Errorf("table %s: condition with operator %s must have a value", tableName, cond.Op))
						}
					case OpIn, OpNotIn:
						if len(cond.Values) == 0 {
							err = errors.Join(err, fmt.Errorf("table %s: condition with operator %s must have values array", tableName, cond.Op))
						}
					case OpIsNull, OpIsNotNull:
						if cond.Value != "" || len(cond.Values) > 0 {
							err = errors.Join(err, fmt.Errorf("table %s: condition with operator %s must not have value or values", tableName, cond.Op))
						}
					case "":
						err = errors.Join(err, fmt.Errorf("table %s: condition must have an operator", tableName))
					default:
						err = errors.Join(err, fmt.Errorf("table %s: unsupported condition operator %s", tableName, cond.Op))
					}
				}
			}
		}

		validateCommands(tableConfig.ResolvedCommands)
		validateCommands(tableConfig.InsertCommands)
		validateCommands(tableConfig.UpdateCommands)
		validateCommands(tableConfig.DeleteCommands)
	}

	if rb.config.Snapshot != nil {
		if e := rb.config.Snapshot.Validate(); e != nil {
			err = errors.Join(err, fmt.Errorf("snapshot: %w", e))
		}
	}

	return err
}

func isSupportedRedisCommand(cmd string) bool {
	supported := map[string]bool{
		"SET": true, "SETEX": true, "MSET": true, "INCR": true, "DECR": true,
		"INCRBY": true, "DECRBY": true, "APPEND": true, "DEL": true, "EXPIRE": true,
		"HSET": true, "HINCRBY": true, "HDEL": true,
		"SADD": true, "SREM": true,
		"ZADD": true, "ZINCRBY": true, "ZREM": true,
		"XADD": true, "XDEL": true,
		"PUBLISH": true,
	}
	return supported[cmd]
}

func (cfg *RedisAppConfig) Settings() appconfig.AppSettings {
	return appconfig.AppSettings{
		StatsInterval:     cfg.StatsInterval,
		MaxWriteQueueSize: cfg.MaxWriteQueueSize,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	}
}

func (cfg *RedisAppConfig) RetryPolicy() appconfig.RetryPolicy {
	return cfg.RedisRetryPolicy
}

func (cfg *RedisAppConfig) ReadRedisOptions(writeTimeout time.Duration) (*redis.Options, error) {
	var dbID int
	if cfg.Redis.Conn.Database != "" {
		if x, err := strconv.Atoi(cfg.Redis.Conn.Database); err != nil {
			dbID = x
		}
	}

	var tlsConfig *tls.Config
	if !cfg.Redis.Conn.TLS.IsEmpty() {
		var err error
		tlsConfig, err = buildRedisTLSConf(cfg.Redis.Conn.TLS)
		if err != nil {
			return nil, err
		}
	}

	return &redis.Options{
		Addr:                  fmt.Sprintf("%s:%s", cfg.Redis.Conn.Host, cfg.Redis.Conn.Port),
		DB:                    dbID,
		Username:              cfg.Redis.Conn.User,
		Password:              cfg.Redis.Conn.Password,
		TLSConfig:             tlsConfig,
		ReadTimeout:           writeTimeout,
		WriteTimeout:          writeTimeout,
		ContextTimeoutEnabled: true,
	}, nil
}

func (cfg *RedisAppConfig) PostgresConfig() pgconfig.PgConfig {
	return cfg.Postgres
}

func (cfg *RedisAppConfig) SnapshotConfig() *appconfig.SnapshotConfig {
	return cfg.Snapshot
}

func (cfg *RedisAppConfig) TablesConfig() map[string]*config.TableConfig {
	m := make(map[string]*config.TableConfig)
	for key, tableConfig := range cfg.Tables {
		m[key] = &config.TableConfig{
			Columns: tableConfig.ExtractColumns(),
			Options: tableConfig.Options,
		}
	}

	return m
}

func (cfg *RedisAppConfig) ValidateAgainstSchema(pubTables map[string]*pgschema.Table) error {
	var err error
	for tableName, tableConfig := range cfg.Tables {
		table, ok := pubTables[tableName]
		if !ok {
			err = errors.Join(err, fmt.Errorf("table %s is not available in publication metadata", tableName))
			continue
		}

		validateCommands := func(commands []RedisCommandWithCondition) {
			for _, cmdWithCond := range commands {
				for _, cmd := range cmdWithCond.Commands {
					for _, arg := range cmd {
						for _, colName := range columnsFromPattern(arg) {
							if !table.HasColumn(colName) {
								err = errors.Join(err, fmt.Errorf("table %s: command references column %q which does not exist", tableName, colName))
							}
						}
					}
				}

				cond := cmdWithCond.Condition
				if cond == nil {
					continue
				}

				condColumn, ok := table.Columns[cond.Column]
				if !ok {
					err = errors.Join(err, fmt.Errorf("table %s: condition column %q does not exist", tableName, cond.Column))
				} else if !conditionOperatorAllowedForType(cond.Op, condColumn.TypeID) {
					err = errors.Join(err, fmt.Errorf("table %s: condition operator %s is not supported for column %q", tableName, cond.Op, cond.Column))
				}

				for _, colName := range columnsFromPattern(cond.Value) {
					if !table.HasColumn(colName) {
						err = errors.Join(err, fmt.Errorf("table %s: condition value references column %q which does not exist", tableName, colName))
					}
				}
				for _, value := range cond.Values {
					for _, colName := range columnsFromPattern(value) {
						if !table.HasColumn(colName) {
							err = errors.Join(err, fmt.Errorf("table %s: condition value references column %q which does not exist", tableName, colName))
						}
					}
				}
			}
		}

		validateCommands(tableConfig.ResolvedCommands)
		validateCommands(tableConfig.InsertCommands)
		validateCommands(tableConfig.UpdateCommands)
		validateCommands(tableConfig.DeleteCommands)
	}
	return err
}

func columnsFromPattern(pattern string) []string {
	cols, all := extractColumnsFromPlaceholder(pattern)
	if all {
		return nil
	}
	return cols
}

func conditionOperatorAllowedForType(op ConditionOperator, typeID uint32) bool {
	if op == OpIsNull || op == OpIsNotNull {
		return true
	}

	if typeID == pgtype.BoolOID {
		return op == OpEqual ||
			op == OpNotEqual ||
			op == OpIn ||
			op == OpNotIn ||
			op == OpIsDistinctFrom
	}

	if typeID == pgtype.JSONOID || typeID == pgtype.JSONBOID {
		return op == OpIsDistinctFrom
	}

	return true
}

func (c *RedisTableConfig) ExtractColumns() []string {
	allCommands := make([]RedisCommandWithCondition, 0)
	allCommands = append(allCommands, c.ResolvedCommands...)
	allCommands = append(allCommands, c.InsertCommands...)
	allCommands = append(allCommands, c.UpdateCommands...)
	allCommands = append(allCommands, c.DeleteCommands...)

	if len(allCommands) == 0 {
		// If NO commands are defined, we need at least PK columns to perform default DEL.
		// But usually ResolvedCommands is defaulted to HSET with {pairs:*} in WellForm.
	}

	columnMap := make(map[string]bool)
	hasAll := false

	for _, cmdWithCond := range allCommands {
		// Extract from command arguments
		for _, cmd := range cmdWithCond.Commands {
			for _, arg := range cmd {
				cols, all := extractColumnsFromPlaceholder(arg)
				if all {
					hasAll = true
				}
				for _, col := range cols {
					columnMap[col] = true
				}
			}
		}

		// Extract from condition
		if cmdWithCond.Condition != nil {
			cond := cmdWithCond.Condition
			// Main column
			cols, _ := extractColumnsFromPlaceholder("{" + cond.Column + "}")
			for _, col := range cols {
				columnMap[col] = true
			}
			// Single value macro
			if cond.Value != "" {
				cols, _ = extractColumnsFromPlaceholder(cond.Value)
				for _, col := range cols {
					columnMap[col] = true
				}
			}
			// Multiple values macros
			for _, val := range cond.Values {
				cols, _ = extractColumnsFromPlaceholder(val)
				for _, col := range cols {
					columnMap[col] = true
				}
			}
		}
	}

	result := make([]string, 0, len(columnMap))
	if hasAll {
		result = append(result, config.AllColumns)
	}

	for col := range columnMap {
		result = append(result, col)
	}
	return result
}

var (
	placeholderRegExp = regexp.MustCompile(`\{([^{}]+)\}`)
)

func extractColumnsFromPlaceholder(s string) (cols []string, all bool) {
	// Find all matches of placeholders like {content}
	matches := placeholderRegExp.FindAllStringSubmatch(s, -1)

	for _, match := range matches {
		placeholder := match[1]

		var content string
		foundPrefix := false
		if strings.HasPrefix(placeholder, "pairs:") {
			content = strings.TrimPrefix(placeholder, "pairs:")
			foundPrefix = true
		} else if strings.HasPrefix(placeholder, "json:") {
			content = strings.TrimPrefix(placeholder, "json:")
			foundPrefix = true
		} else if strings.HasPrefix(placeholder, "columns:") {
			content = strings.TrimPrefix(placeholder, "columns:")
			foundPrefix = true
		} else if strings.HasPrefix(placeholder, "diff:") {
			content = strings.TrimPrefix(placeholder, "diff:")
			foundPrefix = true
		}

		if foundPrefix {
			if content == "*" {
				return nil, true
			}
			parts := strings.Split(content, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					cols = append(cols, p)
				}
			}
		} else {
			// Single column placeholder like {id}
			// But exclude %table%, %schema%, %xid%, %pk%
			placeholder = strings.TrimSpace(placeholder)
			if !strings.HasPrefix(placeholder, "%") && !strings.HasSuffix(placeholder, "%") {
				// Handle {old:column} by extracting only the column name
				if strings.HasPrefix(placeholder, "old:") {
					placeholder = strings.TrimPrefix(placeholder, "old:")
				}
				cols = append(cols, placeholder)
			}
		}
	}
	return cols, false
}

func (cfg *RedisAppConfig) NeedsTrackCommitTimestamp() bool {
	return cfg.Redis.CommitTimeColumn != ""
}

func buildRedisTLSConf(options config.TLSOptions) (*tls.Config, error) {
	caCertPool := x509.NewCertPool()
	caCert, err := os.ReadFile(options.RootCert)

	if err != nil {
		return nil, fmt.Errorf("unable to read root CA certificate: %w", err)
	}

	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("unable to add root CA to certificate pool")
	}

	cert, err := tls.LoadX509KeyPair(options.Cert, options.Key)
	if err != nil {
		return nil, fmt.Errorf("unable to load client certificate: %w", err)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}
