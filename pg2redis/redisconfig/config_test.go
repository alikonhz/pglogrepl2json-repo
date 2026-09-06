package redisconfig

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnErrors(t *testing.T) {
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	hostPort := []struct {
		host  string
		port  string
		name  string
		error string
	}{
		{host: "", port: "5432", name: "no host", error: "no hosts provided"},
		{host: "192.168.1.1", port: "", name: "no port", error: "no ports provided"},
		{host: "192.168.1.1, 192.168.1.2", port: "5432, 5433, 5434", name: "2 hosts 3 ports", error: config.ErrHostsPortsCountMismatch.Error()},
		{host: "192.168.1.1, 192.168.1.2, 192.168.1.3", port: "5432, 5433", name: "3 hosts 2 ports", error: config.ErrHostsPortsCountMismatch.Error()},
	}

	for _, s := range hostPort {
		t.Run(s.name, func(t *testing.T) {
			myCfg := RedisAppConfig{
				Postgres: pgconfig.PgConfig{Conn: &config.ConnectionOptsMany{
					Host:     s.host,
					Port:     s.port,
					Database: "database",
					User:     "user",
					Password: "password",
					TLS: config.TLSOptions{
						Cert:     "cert",
						Key:      "key",
						RootCert: "root",
					},
				}},
			}

			mustWriteToFile(&myCfg, fileName)

			cfg, err := LoadAppConfig(fileName)
			assert.NotNil(t, cfg)
			require.NoError(t, err)
			var all []config.ConnectionOpts
			all, err = cfg.Postgres.Conn.AllOpts()
			assert.Equal(t, 0, len(all))
			assert.Error(t, err)
			assert.Contains(t, err.Error(), s.error)
		})
	}
}

func TestManyPgHostsAndOnePort(t *testing.T) {
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	myCfg := RedisAppConfig{
		Postgres: pgconfig.PgConfig{Conn: &config.ConnectionOptsMany{
			Host:     "192.168.1.1, 192.168.1.2",
			Port:     "5432",
			Database: "database",
			User:     "user",
			Password: "password",
			TLS: config.TLSOptions{
				Cert:     "cert",
				Key:      "key",
				RootCert: "root",
			},
		}},
		Tables: make(map[string]*RedisTableConfig),
	}

	mustWriteToFile(&myCfg, fileName)

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)

	// adjust to default values
	myCfg.Postgres.NumericMode = pgconfig.PgNumericModeFloat

	assert.Equal(t, &myCfg, cfg)

	opts, err := cfg.Postgres.Conn.AllOpts()
	require.NoError(t, err)
	assert.Equal(t, 2, len(opts))

	assert.Equal(t, "192.168.1.1", opts[0].Host)
	assert.Equal(t, "5432", opts[0].Port)
	assert.Equal(t, "192.168.1.2", opts[1].Host)
	assert.Equal(t, "5432", opts[1].Port)
}

func TestManyPgHostsAndPorts(t *testing.T) {
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	myCfg := RedisAppConfig{
		Postgres: pgconfig.PgConfig{Conn: &config.ConnectionOptsMany{
			Host:     "192.168.1.1, 192.168.1.2",
			Port:     "5432, 5433",
			Database: "database",
			User:     "user",
			Password: "password",
			TLS: config.TLSOptions{
				Cert:     "cert",
				Key:      "key",
				RootCert: "root",
			},
		}},
		Tables: make(map[string]*RedisTableConfig),
	}

	mustWriteToFile(&myCfg, fileName)

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)

	// adjust to default values
	myCfg.Postgres.NumericMode = pgconfig.PgNumericModeFloat

	assert.Equal(t, &myCfg, cfg)

	opts, err := cfg.Postgres.Conn.AllOpts()
	require.NoError(t, err)
	assert.Equal(t, 2, len(opts))

	assert.Equal(t, "192.168.1.1", opts[0].Host)
	assert.Equal(t, "5432", opts[0].Port)
	assert.Equal(t, myCfg.Postgres.Conn.Database, opts[0].Database)
	assert.Equal(t, myCfg.Postgres.Conn.User, opts[0].User)
	assert.Equal(t, myCfg.Postgres.Conn.Password, opts[0].Password)
	assert.Equal(t, myCfg.Postgres.Conn.TLS, opts[0].TLS)

	assert.Equal(t, "192.168.1.2", opts[1].Host)
	assert.Equal(t, "5433", opts[1].Port)
	assert.Equal(t, myCfg.Postgres.Conn.Database, opts[1].Database)
	assert.Equal(t, myCfg.Postgres.Conn.User, opts[1].User)
	assert.Equal(t, myCfg.Postgres.Conn.Password, opts[1].Password)
	assert.Equal(t, myCfg.Postgres.Conn.TLS, opts[1].TLS)
}

func TestLoadConfigYamlWithoutSchemaName(t *testing.T) {
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	// when there's no schema name in the table - assume public
	myCfg := RedisAppConfig{
		Postgres: pgconfig.PgConfig{NumericMode: pgconfig.PgNumericModeFloat, Conn: &config.ConnectionOptsMany{}},
		Tables: map[string]*RedisTableConfig{
			"orders": &RedisTableConfig{
				Options: make(map[string]string),
				Commands: []any{
					[]string{"HSET", "orders:{id}", "{pairs:*}"},
				},
			},
		},
	}

	mustWriteToFile(&myCfg, fileName)

	myCfg.Tables["public.orders"] = myCfg.Tables["orders"]
	myCfg.Tables["public.orders"].FullName = "public.orders"
	delete(myCfg.Tables, "orders")

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)

	myCfg.Tables["public.orders"].ResolvedCommands = resolveCommands(myCfg.Tables["public.orders"].Commands)

	for k, v := range cfg.Tables {
		if expected, ok := myCfg.Tables[k]; ok {
			expected.FullName = v.FullName
			expected.ResolvedCommands = v.ResolvedCommands
			expected.Commands = v.Commands
			expected.InsertCommands = v.InsertCommands
			expected.UpdateCommands = v.UpdateCommands
			expected.DeleteCommands = v.DeleteCommands
		}
	}

	assert.Equal(t, &myCfg, cfg)
}

func TestLoadConfigYaml(t *testing.T) {
	clearEnv()
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	myCfg := createMyAppConfig(true)
	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	mustWriteToFile(myCfg, fileName)

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)
	assert.NotNil(t, cfg)

	assert.Equal(t, myCfg, cfg)
}

func TestLoadConfigEnv(t *testing.T) {
	clearEnv()
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)
	myCfg := createMyAppConfig(true)
	mustWriteToEnv(myCfg)

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)
	assert.NotNil(t, cfg)

	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	compareConfigs(t, myCfg, cfg)
}

func TestZIncrByEmptyKey(t *testing.T) {
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	myCfg := createMyAppConfig(true)
	myCfg.Tables["public.orders"].Commands = []any{[]string{"UNSUPPORTED_COMMAND", "key"}}

	mustWriteToFile(myCfg, fileName)

	_, err := LoadAppConfigNoEnv(fileName)
	assert.Error(t, err)
}

func TestZIncrBy(t *testing.T) {
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)
	myCfg := createMyAppConfig(true)
	myCfg.Tables["public.orders"].Options[ZIncrByKeyOptName] = "id"
	myCfg.Tables["public.orders"].Options[ZIncrByIncrOptName] = "id"
	myCfg.Tables["public.orders"].Commands = []any{[]string{"ZADD", "leaderboard", "{score}", "{id}"}}

	mustWriteToFile(myCfg, fileName)

	cfg, err := LoadAppConfigNoEnv(fileName)
	require.NoError(t, err)

	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	compareConfigs(t, myCfg, cfg)
}

func TestReadRedisOptionsAppliesTimeouts(t *testing.T) {
	cfg := &RedisAppConfig{
		Redis: RedisConfig{
			Conn: config.ConnectionOpts{
				Host:     "localhost",
				Port:     "6379",
				User:     "redisuser",
				Password: "redispwd",
			},
		},
	}

	timeout := 7 * time.Second
	opts, err := cfg.ReadRedisOptions(timeout)

	require.NoError(t, err)
	assert.Equal(t, timeout, opts.ReadTimeout)
	assert.Equal(t, timeout, opts.WriteTimeout)
	assert.True(t, opts.ContextTimeoutEnabled)
}

func TestFlushSettingsIncludesBufferSize(t *testing.T) {
	flushSettings, err := appconfig.NewFlushOptions("250ms", 123, 7, 2, "3s")
	require.NoError(t, err)

	assert.Equal(t, 250*time.Millisecond, flushSettings.Interval)
	assert.Equal(t, uint32(123), flushSettings.BufferSize)
	assert.Equal(t, uint32(7), flushSettings.QueueDepth)
	assert.Equal(t, uint32(2), flushSettings.Workers)
	assert.Equal(t, 3*time.Second, flushSettings.Timeout)
}

func createMyAppConfig(createTables bool) *RedisAppConfig {
	replFailover := true
	myCfg := &RedisAppConfig{
		Redis: RedisConfig{
			Conn: config.ConnectionOpts{
				Host:     "redishost",
				Port:     "redisport",
				Database: "redisdb",
				User:     "redisuser",
				Password: "redispwd",
			},
		},
		Postgres: pgconfig.PgConfig{
			Conn: &config.ConnectionOptsMany{
				Host:     "pghost",
				Port:     "pgport",
				Database: "pgdb",
				User:     "pguser",
				Password: "pgpwd",
				TLS: config.TLSOptions{
					Cert:     "cert",
					Key:      "key",
					RootCert: "rootCert",
				},
			},
			Repl: pgconfig.PgReplConfig{
				Slot:     "pgslot",
				Pub:      "pgpub",
				Owner:    pgconfig.OwnerApp,
				Failover: &replFailover,
			},
			NumericMode: pgconfig.PgNumericModeFloat,
		},
		Tables: map[string]*RedisTableConfig{
			"public.orders": &RedisTableConfig{
				Options: map[string]string{},
				Commands: createCommands([]RedisCommandWithCondition{
					{
						Commands: [][]string{{"HSET", "orders:{id}", "{pairs:*}"}},
						Condition: &ConditionConfig{
							Op:     OpIn,
							Column: "state",
							Values: []string{"new", "cancelled"},
						},
					},
				}),
				Update: &TableOperationConfig{
					Commands: createCommands([]RedisCommandWithCondition{
						{
							Commands: [][]string{{"HSET", "orders:{id}", "{pairs:*}"}},
							Condition: &ConditionConfig{
								Op:     OpEqual,
								Column: "state",
								Value:  "ordered",
							},
						},
					}),
				},
			},
		},
	}

	if !createTables {
		myCfg.Tables = make(map[string]*RedisTableConfig)
	}

	return myCfg
}

func createCommands(commands []RedisCommandWithCondition) []any {
	cmds := make([]any, len(commands))
	for i, cmd := range commands {
		allCmdParts := make([]any, len(cmd.Commands))
		for k, singleCmd := range cmd.Commands {
			cmdParts := make([]any, len(singleCmd))
			for j, v := range singleCmd {
				cmdParts[j] = v
			}
			allCmdParts[k] = cmdParts
		}

		var cmdToUse any
		if len(allCmdParts) == 1 {
			cmdToUse = allCmdParts[0]
		} else {
			cmdToUse = allCmdParts
		}

		if cmd.Condition == nil {
			cmds[i] = cmdToUse
		} else {
			cmdMap := map[string]any{
				"command": cmdToUse,
				"condition": map[string]any{
					"op":     string(cmd.Condition.Op),
					"column": cmd.Condition.Column,
				},
			}
			if cmd.Condition.Value != "" {
				cmdMap["condition"].(map[string]any)["value"] = cmd.Condition.Value
			}
			if len(cmd.Condition.Values) > 0 {
				vals := make([]any, len(cmd.Condition.Values))
				for j, v := range cmd.Condition.Values {
					vals[j] = v
				}
				cmdMap["condition"].(map[string]any)["values"] = vals
			}
			cmds[i] = cmdMap
		}
	}

	return cmds
}

func TestSchemaInCommand(t *testing.T) {
	// The issue: When I have a command e.g. HSET,customers:{%pk%},{pairs:*}
	// the application automatically adds schema and sends actual command like HSET,chaos.customers:{%pk%},{pairs:*}
	// It should NOT add schema unless %schema% placeholder is used.

	myCfg := &RedisAppConfig{
		Tables: map[string]*RedisTableConfig{
			"chaos.customers": &RedisTableConfig{
				Commands: []any{
					[]string{"HSET", "customers:{%pk%}", "{pairs:*}"},
				},
			},
		},
	}

	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	table, ok := myCfg.Tables["chaos.customers"]
	require.True(t, ok)
	assert.Equal(t, "customers:{%pk%}", table.ResolvedCommands[0].Commands[0][1], "Should not add schema to user-defined command")
}

func mustWriteToEnv(a *RedisAppConfig) {
	const (
		pg = "POSTGRES"
		rd = "REDIS"
	)

	builder := &RedisAppConfigBuilder{config: a}
	builder.WellForm(nil)

	writeConnToEnv(pg, a.Postgres.Conn.AsOpt())

	os.Setenv(fmt.Sprintf("%s_%s_REPL_PUB", pg2redisPrefix, pg), a.Postgres.Repl.Pub)
	os.Setenv(fmt.Sprintf("%s_%s_REPL_SLOT", pg2redisPrefix, pg), a.Postgres.Repl.Slot)
	os.Setenv(fmt.Sprintf("%s_%s_REPL_OWNER", pg2redisPrefix, pg), string(a.Postgres.Repl.Owner))
	if a.Postgres.Repl.Failover != nil {
		os.Setenv(fmt.Sprintf("%s_%s_REPL_FAILOVER", pg2redisPrefix, pg), strconv.FormatBool(*a.Postgres.Repl.Failover))
	}
	os.Setenv(fmt.Sprintf("%s_%s_NUMERICMODE", pg2redisPrefix, pg), pgconfig.PgNumericModeFloat)
	writeConnToEnv(rd, a.Redis.Conn)

	// write tables into env using indexed variables: PG2REDIS_T_<idx>_NAME
	idx := 0
	for name, table := range a.Tables {
		prefix := fmt.Sprintf("%s_T_%d", pg2redisPrefix, idx)
		os.Setenv(fmt.Sprintf("%s_NAME", prefix), name)

		writeCommandsWithConditionToEnv(fmt.Sprintf("%s_COMMANDS", prefix), table.ResolvedCommands)
		if table.Insert != nil {
			writeCommandsWithConditionToEnv(fmt.Sprintf("%s_INSERT_COMMANDS", prefix), table.InsertCommands)
		}
		if table.Update != nil {
			writeCommandsWithConditionToEnv(fmt.Sprintf("%s_UPDATE_COMMANDS", prefix), table.UpdateCommands)
		}
		if table.Delete != nil {
			writeCommandsWithConditionToEnv(fmt.Sprintf("%s_DELETE_COMMANDS", prefix), table.DeleteCommands)
		}

		for k, v := range table.Options {
			os.Setenv(fmt.Sprintf("%s_OPTIONS_%s", prefix, strings.ToUpper(k)), v)
		}

		idx++
	}
}

func writeCommandsWithConditionToEnv(prefix string, commands []RedisCommandWithCondition) {
	for i, cmd := range commands {
		base := fmt.Sprintf("%s_%d", prefix, i)
		// For tests, we only handle the first command in the group when writing to env
		// because the env format is limited.
		if cmd.Condition == nil {
			os.Setenv(base, strings.Join(cmd.Commands[0], "/"))
		} else {
			for j, part := range cmd.Commands[0] {
				os.Setenv(fmt.Sprintf("%s_COMMAND_%d", base, j), part)
			}
			os.Setenv(fmt.Sprintf("%s_CONDITION_OP", base), string(cmd.Condition.Op))
			os.Setenv(fmt.Sprintf("%s_CONDITION_COLUMN", base), cmd.Condition.Column)
			if cmd.Condition.Value != "" {
				os.Setenv(fmt.Sprintf("%s_CONDITION_VALUE", base), cmd.Condition.Value)
			}
			for j, v := range cmd.Condition.Values {
				os.Setenv(fmt.Sprintf("%s_CONDITION_VALUES_%d", base, j), v)
			}
		}
	}
}

func writeConnToEnv(connPrefix string, conn config.ConnectionOpts) {
	os.Setenv(fmt.Sprintf("%s_%s_CONN_HOST", pg2redisPrefix, connPrefix), conn.Host)
	os.Setenv(fmt.Sprintf("%s_%s_CONN_PORT", pg2redisPrefix, connPrefix), conn.Port)
	os.Setenv(fmt.Sprintf("%s_%s_CONN_DATABASE", pg2redisPrefix, connPrefix), conn.Database)
	os.Setenv(fmt.Sprintf("%s_%s_CONN_PORT", pg2redisPrefix, connPrefix), conn.Port)
	os.Setenv(fmt.Sprintf("%s_%s_CONN_PASSWORD", pg2redisPrefix, connPrefix), conn.Password)
	os.Setenv(fmt.Sprintf("%s_%s_CONN_USER", pg2redisPrefix, connPrefix), conn.User)
	if !conn.TLS.IsEmpty() {
		os.Setenv(fmt.Sprintf("%s_%s_CONN_TLS_CERT", pg2redisPrefix, connPrefix), conn.TLS.Cert)
		os.Setenv(fmt.Sprintf("%s_%s_CONN_TLS_KEY", pg2redisPrefix, connPrefix), conn.TLS.Key)
		os.Setenv(fmt.Sprintf("%s_%s_CONN_TLS_ROOTCERT", pg2redisPrefix, connPrefix), conn.TLS.RootCert)
	}
}

func mustWriteToFile(a *RedisAppConfig, fileName string) {
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "postgres:")
	fmt.Fprintln(w, "  repl:")
	fmt.Fprintf(w, "    pub: %s\n", a.Postgres.Repl.Pub)
	fmt.Fprintf(w, "    slot: %s\n", a.Postgres.Repl.Slot)
	fmt.Fprintf(w, "    owner: %s\n", a.Postgres.Repl.Owner)
	if a.Postgres.Repl.Failover != nil {
		fmt.Fprintf(w, "    failover: %t\n", *a.Postgres.Repl.Failover)
	}
	writeConn(w, a.Postgres.Conn.AsOpt())
	fmt.Fprintf(w, "  numericMode: %s\n", a.Postgres.NumericMode)

	fmt.Fprintln(w, "redis:")
	writeConn(w, a.Redis.Conn)

	fmt.Fprintln(w, "tables:")
	for key, tableConfig := range a.Tables {
		fmt.Fprintf(w, "  - %s:\n", key)

		writeTableOperation(w, "      ", "commands", tableConfig.Commands)
		if tableConfig.Insert != nil {
			fmt.Fprintln(w, "      insert:")
			writeTableOperation(w, "        ", "commands", tableConfig.Insert.Commands)
		}
		if tableConfig.Update != nil {
			fmt.Fprintln(w, "      update:")
			writeTableOperation(w, "        ", "commands", tableConfig.Update.Commands)
		}
		if tableConfig.Delete != nil {
			fmt.Fprintln(w, "      delete:")
			writeTableOperation(w, "        ", "commands", tableConfig.Delete.Commands)
		}

		if len(tableConfig.Options) > 0 {
			fmt.Fprintln(w, "      options:")
			for k, v := range tableConfig.Options {
				fmt.Fprintf(w, "        %s: %s\n", k, v)
			}
		}
	}
	err = w.Flush()
	if err != nil {
		panic(err)
	}
}

func writeTableOperation(w *bufio.Writer, indent, name string, commands []any) {
	if len(commands) == 0 {
		return
	}
	fmt.Fprintf(w, "%s%s:\n", indent, name)
	for _, cmd := range commands {
		switch c := cmd.(type) {
		case []string:
			fmt.Fprintf(w, "%s  - [\"%s\"]\n", indent, strings.Join(c, "\", \""))
		case []any:
			parts := make([]string, len(c))
			for i, p := range c {
				parts[i] = fmt.Sprintf("%v", p)
			}
			fmt.Fprintf(w, "%s  - [\"%s\"]\n", indent, strings.Join(parts, "\", \""))
		case map[string]any:
			cmdPartsRaw := c["command"]
			var parts []string
			switch cp := cmdPartsRaw.(type) {
			case []string:
				parts = cp
			case []any:
				parts = make([]string, len(cp))
				for i, p := range cp {
					parts[i] = fmt.Sprintf("%v", p)
				}
			}
			fmt.Fprintf(w, "%s  - command: [\"%s\"]\n", indent, strings.Join(parts, "\", \""))
			if cond, ok := c["condition"].(map[string]any); ok {
				fmt.Fprintf(w, "%s    condition:\n", indent)
				fmt.Fprintf(w, "%s      op: \"%s\"\n", indent, cond["op"])
				fmt.Fprintf(w, "%s      column: \"%s\"\n", indent, cond["column"])
				if val, ok := cond["value"].(string); ok && val != "" {
					fmt.Fprintf(w, "%s      value: \"%s\"\n", indent, val)
				}
				if valsRaw, ok := cond["values"]; ok {
					var vals []string
					switch v := valsRaw.(type) {
					case []string:
						vals = v
					case []any:
						vals = make([]string, len(v))
						for i, p := range v {
							vals[i] = fmt.Sprintf("%v", p)
						}
					}
					if len(vals) > 0 {
						fmt.Fprintf(w, "%s      values: [\"%s\"]\n", indent, strings.Join(vals, "\", \""))
					}
				}
			}
		}
	}
}

func TestResolveCommandsLegacy(t *testing.T) {
	cmds := []any{[]string{"HSET", "k", "v"}}
	res := resolveCommands(cmds)
	assert.Len(t, res, 1)
	assert.Len(t, res[0].Commands, 1)
	assert.Equal(t, "HSET", res[0].Commands[0][0])
}

func TestResolveCommandsLegacyAny(t *testing.T) {
	cmds := []any{[]any{"HSET", "k", "v"}}
	res := resolveCommands(cmds)
	assert.Len(t, res, 1)
	assert.Len(t, res[0].Commands, 1)
	assert.Equal(t, "HSET", res[0].Commands[0][0])
}
func TestMultiCommandParsing(t *testing.T) {
	clearEnv()
	yaml := `
postgres:
  conn:
    host: localhost
  repl:
    pub: p
    slot: s
    owner: app
redis:
  conn:
    host: localhost
tables:
  - public.orders:
      commands:
        - command:
            - ["HSET", "order:{id}", "status", "{status}"]
            - ["PUBLISH", "order_updates", "{id}"]
          condition:
            column: "status"
            op: "="
            value: "completed"
`
	fileName := "test_multi_cmd.yaml"
	os.WriteFile(fileName, []byte(yaml), 0644)
	defer os.Remove(fileName)

	cfg, err := LoadAppConfigNoEnv(fileName)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	table, ok := cfg.Tables["public.orders"]
	require.True(t, ok)
	require.Len(t, table.ResolvedCommands, 1)

	cmdWithCond := table.ResolvedCommands[0]
	assert.Len(t, cmdWithCond.Commands, 2)
	assert.Equal(t, "HSET", cmdWithCond.Commands[0][0])
	assert.Equal(t, "PUBLISH", cmdWithCond.Commands[1][0])
	assert.NotNil(t, cmdWithCond.Condition)
	assert.Equal(t, "status", cmdWithCond.Condition.Column)
}
func TestWellFormLegacy(t *testing.T) {
	myCfg := &RedisAppConfig{
		Tables: map[string]*RedisTableConfig{
			"public.users": {
				Commands: []any{[]string{"PUBLISH", "topic", "msg"}},
			},
		},
	}
	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	table := myCfg.Tables["public.users"]
	assert.Len(t, table.ResolvedCommands, 1)
	assert.Len(t, table.ResolvedCommands[0].Commands, 1)
	assert.Equal(t, "PUBLISH", table.ResolvedCommands[0].Commands[0][0])
}
func compareConfigs(t *testing.T, expected, actual *RedisAppConfig) {
	for name, expectedTable := range expected.Tables {
		actualTable, ok := actual.Tables[name]
		require.True(t, ok, "Table %s missing in actual", name)

		assert.Equal(t, expectedTable.ResolvedCommands, actualTable.ResolvedCommands, "ResolvedCommands mismatch for table %s", name)
		assert.Equal(t, expectedTable.InsertCommands, actualTable.InsertCommands, "InsertCommands mismatch for table %s", name)
		assert.Equal(t, expectedTable.UpdateCommands, actualTable.UpdateCommands, "UpdateCommands mismatch for table %s", name)
		assert.Equal(t, expectedTable.DeleteCommands, actualTable.DeleteCommands, "DeleteCommands mismatch for table %s", name)

		// Clear fields that are hard to compare due to type differences in raw unmarshaling
		expectedTable.Commands = nil
		expectedTable.ResolvedCommands = nil
		expectedTable.InsertCommands = nil
		expectedTable.UpdateCommands = nil
		expectedTable.DeleteCommands = nil
		expectedTable.Insert = nil
		expectedTable.Update = nil
		expectedTable.Delete = nil

		actualTable.Commands = nil
		actualTable.ResolvedCommands = nil
		actualTable.InsertCommands = nil
		actualTable.UpdateCommands = nil
		actualTable.DeleteCommands = nil
		actualTable.Insert = nil
		actualTable.Update = nil
		actualTable.Delete = nil
	}

	assert.Equal(t, expected, actual)
}

func TestLoadConditionalConfigYaml(t *testing.T) {
	clearEnv()
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	myCfg := &RedisAppConfig{
		Postgres: pgconfig.PgConfig{Conn: &config.ConnectionOptsMany{}, NumericMode: pgconfig.PgNumericModeFloat},
		Tables: map[string]*RedisTableConfig{
			"public.orders": {
				Commands: createCommands([]RedisCommandWithCondition{
					{
						Commands: [][]string{{"PUBLISH", "order_status", "{id}:{status}"}},
						Condition: &ConditionConfig{
							Op:     OpIsDistinctFrom,
							Column: "status",
							Value:  "{old:status}",
						},
					},
					{
						Commands: [][]string{{"HSET", "orders:{id}", "completed", "1"}},
						Condition: &ConditionConfig{
							Op:     OpIsNotNull,
							Column: "completed_at",
						},
					},
				}),
				Insert: &TableOperationConfig{
					Commands: createCommands([]RedisCommandWithCondition{
						{
							Commands: [][]string{{"SADD", "new_orders", "{id}"}},
							Condition: &ConditionConfig{
								Op:     OpEqual,
								Column: "state",
								Value:  "new",
							},
						},
					}),
				},
			},
		},
	}

	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	mustWriteToFile(myCfg, fileName)

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)
	assert.NotNil(t, cfg)

	compareConfigs(t, myCfg, cfg)
}

func TestLoadConditionalConfigEnv(t *testing.T) {
	clearEnv()
	myCfg := &RedisAppConfig{
		Postgres: pgconfig.PgConfig{Conn: &config.ConnectionOptsMany{}, NumericMode: pgconfig.PgNumericModeFloat},
		Tables: map[string]*RedisTableConfig{
			"public.products": {
				Commands: createCommands([]RedisCommandWithCondition{
					{
						Commands: [][]string{{"HSET", "products:{id}", "on_sale", "1"}},
						Condition: &ConditionConfig{
							Op:     OpLess,
							Column: "price",
							Value:  "100.0",
						},
					},
				}),
			},
		},
	}

	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	mustWriteToEnv(myCfg)
	defer clearEnv()

	cfg, err := LoadAppConfig("non-existent.yaml") // Force load from env only
	require.NoError(t, err)
	assert.NotNil(t, cfg)

	compareConfigs(t, myCfg, cfg)
}

func TestInvalidConditionConfig(t *testing.T) {
	tests := []struct {
		name      string
		condition *ConditionConfig
		wantErr   string
	}{
		{
			name: "missing operator",
			condition: &ConditionConfig{
				Column: "status",
				Value:  "active",
			},
			wantErr: "condition must have an operator",
		},
		{
			name: "missing value for equality",
			condition: &ConditionConfig{
				Op:     OpEqual,
				Column: "status",
			},
			wantErr: "condition with operator = must have a value",
		},
		{
			name: "missing values for in",
			condition: &ConditionConfig{
				Op:     OpIn,
				Column: "status",
			},
			wantErr: "condition with operator in must have values array",
		},
		{
			name: "unsupported operator",
			condition: &ConditionConfig{
				Op:     "LIKE",
				Column: "name",
				Value:  "test%",
			},
			wantErr: "unsupported condition operator LIKE",
		},
		{
			name: "is_null with value",
			condition: &ConditionConfig{
				Op:     OpIsNull,
				Column: "deleted_at",
				Value:  "anything",
			},
			wantErr: "condition with operator is_null must not have value or values",
		},
		{
			name: "is_not_null with values",
			condition: &ConditionConfig{
				Op:     OpIsNotNull,
				Column: "deleted_at",
				Values: []string{"anything"},
			},
			wantErr: "condition with operator is_not_null must not have value or values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			myCfg := &RedisAppConfig{
				Tables: map[string]*RedisTableConfig{
					"public.test": {
						ResolvedCommands: []RedisCommandWithCondition{
							{
								Commands:  [][]string{{"HSET", "key", "field", "val"}},
								Condition: tt.condition,
							},
						},
					},
				},
			}

			builder := &RedisAppConfigBuilder{config: myCfg}
			err := builder.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestInvalidCommandConfigFailsLoad(t *testing.T) {
	clearEnv()
	tests := []struct {
		name     string
		commands string
		wantErr  string
	}{
		{
			name:     "invalid top-level command",
			commands: "        - 42\n",
			wantErr:  "invalid command format",
		},
		{
			name:     "legacy command with non-string argument",
			commands: "        - [\"HSET\", \"orders:{id}\", 123]\n",
			wantErr:  "command argument 2 is not a string",
		},
		{
			name: "conditional command with non-string argument",
			commands: `        - command: ["HSET", "orders:{id}", 123]
          condition:
            column: "status"
            op: "="
            value: "completed"
`,
			wantErr: "command argument 2 is not a string",
		},
		{
			name: "multi-command with non-string argument",
			commands: `        - command:
            - ["HSET", "orders:{id}", "status", "{status}"]
            - ["PUBLISH", "order_updates", 123]
          condition:
            column: "status"
            op: "="
            value: "completed"
`,
			wantErr: "command 1 argument 2 is not a string",
		},
		{
			name: "missing conditional command",
			commands: `        - condition:
            column: "status"
            op: "="
            value: "completed"
`,
			wantErr: "conditional command missing command",
		},
		{
			name: "empty command group",
			commands: `        - command: []
          condition:
            column: "status"
            op: "="
            value: "completed"
`,
			wantErr: "command group cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileName := strings.ReplaceAll(t.Name(), "/", "_") + ".yaml"
			defer os.Remove(fileName)

			yaml := `postgres:
  conn:
    host: localhost
  repl:
    pub: p
    slot: s
    owner: app
redis:
  conn:
    host: localhost
tables:
  - public.orders:
      commands:
` + tt.commands

			require.NoError(t, os.WriteFile(fileName, []byte(yaml), 0644))

			_, err := LoadAppConfigNoEnv(fileName)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "table public.orders commands")
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConditionalConfigAllOperatorsYaml(t *testing.T) {
	clearEnv()
	fileName := fmt.Sprintf("%s.yaml", t.Name())
	os.Remove(fileName)

	myCfg := &RedisAppConfig{
		Postgres: pgconfig.PgConfig{Conn: &config.ConnectionOptsMany{}, NumericMode: pgconfig.PgNumericModeFloat},
		Tables: map[string]*RedisTableConfig{
			"public.all_ops": {
				Commands: createCommands([]RedisCommandWithCondition{
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpEqual, Column: "c", Value: "v"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpNotEqual, Column: "c", Value: "v"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpLess, Column: "c", Value: "10"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpGreater, Column: "c", Value: "10"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpLessEq, Column: "c", Value: "10"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpGreaterEq, Column: "c", Value: "10"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpIn, Column: "c", Values: []string{"a", "b"}}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpNotIn, Column: "c", Values: []string{"a", "b"}}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpIsNull, Column: "c"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpIsNotNull, Column: "c"}},
					{Commands: [][]string{{"HSET", "k", "f", "v"}}, Condition: &ConditionConfig{Op: OpIsDistinctFrom, Column: "c", Value: "{old:c}"}},
				}),
			},
		},
	}

	builder := &RedisAppConfigBuilder{config: myCfg}
	builder.WellForm(nil)

	mustWriteToFile(myCfg, fileName)

	cfg, err := LoadAppConfig(fileName)
	require.NoError(t, err)
	assert.NotNil(t, cfg)

	compareConfigs(t, myCfg, cfg)
}

func clearEnv() {
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, pg2redisPrefix+"_") {
			parts := strings.SplitN(env, "=", 2)
			os.Unsetenv(parts[0])
		}
	}
}

func writeConn(w *bufio.Writer, conn config.ConnectionOpts) {
	const (
		indent   = "  "
		indent2x = indent + indent
		indent3x = indent + indent + indent
	)
	fmt.Fprintf(w, "%sconn:\n", indent)
	fmt.Fprintf(w, "%sdatabase: %s\n", indent2x, conn.Database)
	fmt.Fprintf(w, "%shost: %s\n", indent2x, conn.Host)
	fmt.Fprintf(w, "%sport: %s\n", indent2x, conn.Port)
	fmt.Fprintf(w, "%suser: %s\n", indent2x, conn.User)
	fmt.Fprintf(w, "%spassword: %s\n", indent2x, conn.Password)
	if !conn.TLS.IsEmpty() {
		fmt.Fprintf(w, "%stls:\n", indent2x)
		fmt.Fprintf(w, "%scert: %s\n", indent3x, conn.TLS.Cert)
		fmt.Fprintf(w, "%skey: %s\n", indent3x, conn.TLS.Key)
		fmt.Fprintf(w, "%srootCert: %s\n", indent3x, conn.TLS.RootCert)
	}
}
