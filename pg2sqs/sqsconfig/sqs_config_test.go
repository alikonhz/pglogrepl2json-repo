package sqsconfig

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQSRetryPolicyToRetryConfig(t *testing.T) {
	rp := appconfig.RetryPolicy{
		MaxRetries:     10,
		InitialBackoff: "100ms",
		Multiplier:     2.3,
		Jitter:         0.88,
		MaxBackoff:     "10",
	}

	cfg, err := rp.AsRetryConfig()

	require.NoError(t, err)

	assert.Equal(t, 10, cfg.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.InitialBackoff)
	assert.Equal(t, 2.3, cfg.Multiplier)
	assert.Equal(t, 0.88, cfg.Jitter)
	assert.Equal(t, 10*time.Second, cfg.MaxBackoff)
}

func TestSQSRetryPolicyValues(t *testing.T) {
	invalid := []appconfig.RetryPolicy{
		// max retries must be >= 1
		{MaxRetries: 0, InitialBackoff: "5s", Multiplier: 1, Jitter: 0.4, MaxBackoff: "10s"},
		{MaxRetries: -1, InitialBackoff: "5s", Multiplier: 1, Jitter: 0.4, MaxBackoff: "10s"},

		// initialBackoff must be > 0
		{MaxRetries: 1, InitialBackoff: "0", Multiplier: 1, Jitter: 0.4, MaxBackoff: "10s"},
		{MaxRetries: 1, InitialBackoff: "-2", Multiplier: 1, Jitter: 0.4, MaxBackoff: "10s"},

		// multiplier must be >= 1
		{MaxRetries: 1, InitialBackoff: "5s", Multiplier: 0.4, Jitter: 0.4, MaxBackoff: "10s"},
		{MaxRetries: 1, InitialBackoff: "5s", Multiplier: -0.4, Jitter: 0.4, MaxBackoff: "10s"},

		// jitter must be >= 0
		{MaxRetries: 1, InitialBackoff: "5s", Multiplier: 0.4, Jitter: 0, MaxBackoff: "10s"},
		{MaxRetries: 1, InitialBackoff: "5s", Multiplier: 0.4, Jitter: -1.2, MaxBackoff: "10s"},

		// maxBackoff must be > 0
		{MaxRetries: 1, InitialBackoff: "5s", Multiplier: 0.4, Jitter: 0, MaxBackoff: "0"},
		{MaxRetries: 1, InitialBackoff: "5s", Multiplier: 0.4, Jitter: 0, MaxBackoff: "-20"},
	}

	for i, policy := range invalid {
		_, err := policy.AsRetryConfig()
		require.Error(t, err, "policy: %+v, index: %d", policy, i)
	}

	valid := []appconfig.RetryPolicy{
		// we accept maxRetries == 1 only when all other values are empty
		{MaxRetries: 1, InitialBackoff: "", Multiplier: 0, Jitter: 0, MaxBackoff: ""},
	}

	for _, policy := range valid {
		_, err := policy.AsRetryConfig()
		require.NoError(t, err, policy)
	}
}

func TestSQSRetryPolicyToRetryConfigError(t *testing.T) {
	invalidValues := []string{
		"asd",
		"---",
		"",
	}

	for _, value := range invalidValues {
		rp := appconfig.RetryPolicy{
			MaxRetries:     1,
			InitialBackoff: value,
			Multiplier:     2,
			Jitter:         3,
			MaxBackoff:     value,
		}

		_, err := rp.AsRetryConfig()
		assert.Error(t, err)
	}
}

func TestSQSConfig(t *testing.T) {
	fileName := t.Name() + ".yaml"

	os.Remove(fileName)

	cfg := newSQSAppConfig()

	mustWriteToFile(cfg, fileName)

	cfgFile, err := LoadAppConfigNoEnv(fileName)
	require.NoError(t, err)
	assert.Equal(t, cfg, cfgFile)
}

func TestSQSFromEnv(t *testing.T) {
	env := map[string]string{
		"PG2SQS_T_0_NAME":             "customers",
		"PG2SQS_T_0_COLUMNS":          "col1,col2",
		"PG2SQS_T_0_Q_INSERT_NAME":    "customersInsert",
		"PG2SQS_T_0_Q_INSERT_GROUPID": "${%table%}${id}-insert",
		"PG2SQS_T_0_Q_UPDATE_NAME":    "customersUpdate",
		"PG2SQS_T_0_Q_UPDATE_GROUPID": "${%table%}${id}-update",
		"PG2SQS_T_0_Q_DELETE_NAME":    "customersDelete",
		"PG2SQS_T_0_Q_DELETE_GROUPID": "${%table%}${id}-delete",

		"PG2SQS_T_1_NAME":          "orders",
		"PG2SQS_T_1_COLUMNS":       "orderid,orderdate",
		"PG2SQS_T_1_Q_NAME":        "queueName",
		"PG2SQS_T_1_Q_DELETE_NAME": "skip",
	}

	for key, value := range env {
		t.Setenv(key, value)
	}

	config, err := LoadAppConfig("")
	require.NoError(t, err)

	require.NotNil(t, config)
	require.NotNil(t, config.Tables)
	assert.Len(t, config.Tables, 2)

	customersTable, ok := config.Tables["public.customers"]
	require.True(t, ok)

	assert.Equal(t, []string{"col1", "col2"}, customersTable.Columns)
	assert.Equal(t, "customersInsert", customersTable.QueueConfig.Insert.Name)
	assert.Equal(t, "${%table%}${id}-insert", customersTable.QueueConfig.Insert.GroupID)
	assert.Equal(t, "customersUpdate", customersTable.QueueConfig.Update.Name)
	assert.Equal(t, "${%table%}${id}-update", customersTable.QueueConfig.Update.GroupID)
	assert.Equal(t, "customersDelete", customersTable.QueueConfig.Delete.Name)
	assert.Equal(t, "${%table%}${id}-delete", customersTable.QueueConfig.Delete.GroupID)

	ordersTable, ok := config.Tables["public.orders"]
	require.True(t, ok)
	assert.Equal(t, []string{"orderid", "orderdate"}, ordersTable.Columns)
	assert.Equal(t, "queueName", ordersTable.QueueConfig.Name)
	assert.Equal(t, "skip", ordersTable.QueueConfig.Delete.Name)
}

func newSQSAppConfig() *SQSAppConfig {
	cfg := &SQSAppConfig{
		SQS: SQSConfig{
			CommitTimeColumn: "column1",
		},
		Postgres: pgconfig.PgConfig{
			NumericMode: "float",
		},
		Debug: true,
		SQSRetryPolicy: appconfig.RetryPolicy{
			MaxRetries:     5,
			InitialBackoff: fmt.Sprintf("%.0f", (5 * time.Second).Seconds()),
			Multiplier:     2,
			Jitter:         0.1,
			MaxBackoff:     fmt.Sprintf("%.0f", (15 * time.Second).Seconds()),
		},
		Tables: map[string]*SQSTableConfig{
			"public.orders": &SQSTableConfig{
				Columns: []string{"id", "date"},
				Options: map[string]string{
					"option1": "value1",
				},
				QueueConfig: &SQSQueueConfig{
					Name: "default-name",
					Insert: &SQSQueueConfig{
						Name: "orderCreated.fifo",
					},
					Update: &SQSQueueConfig{
						Name: "orderUpdated.fifo",
					},
					Delete: &SQSQueueConfig{
						Name: "orderDeleted.fifo",
					},
				},
			},
		},
	}
	return cfg
}

func NoTestKoanf(t *testing.T) {
	k := koanf.New(".")
	err := k.Load(file.Provider("TestSQSConfig.yaml"), yaml.Parser())
	require.NoError(t, err)

	var actual SQSAppConfig
	err = k.UnmarshalWithConf("", &actual, koanf.UnmarshalConf{Tag: "json"})

	require.NoError(t, err)

	expected := newSQSAppConfig()

	assert.Same(t, expected, &actual)
}

func mustWriteToFile(cfg *SQSAppConfig, fileName string) {
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "postgres:")
	fmt.Fprintln(w, "  numericMode: float")
	fmt.Fprintf(w, "debug: %t\n", cfg.Debug)
	fmt.Fprintln(w, "sqs:")
	fmt.Fprintf(w, "  commitTimeColumn: %s\n", cfg.SQS.CommitTimeColumn)

	fmt.Fprintln(w, "retryPolicy:")
	fmt.Fprintf(w, "  maxRetries: %d\n", cfg.SQSRetryPolicy.MaxRetries)
	fmt.Fprintf(w, "  initialBackoff: %s\n", cfg.SQSRetryPolicy.InitialBackoff)
	fmt.Fprintf(w, "  multiplier: %.2f\n", cfg.SQSRetryPolicy.Multiplier)
	fmt.Fprintf(w, "  jitter: %.2f\n", cfg.SQSRetryPolicy.Jitter)
	fmt.Fprintf(w, "  maxBackoff: %s\n", cfg.SQSRetryPolicy.MaxBackoff)

	fmt.Fprintln(w, "tables:")

	for key, tableConfig := range cfg.Tables {
		fmt.Fprintf(w, "  %s:\n", key)
		fmt.Fprintf(w, "    columns: [%s]\n", strings.Join(tableConfig.Columns, ","))

		if len(tableConfig.Options) > 0 {
			fmt.Fprintln(w, "    options:")

			for k, v := range tableConfig.Options {
				fmt.Fprintf(w, "      %s: %s\n", k, v)
			}
		}

		if tableConfig.QueueConfig != nil {
			fmt.Fprintln(w, "    queue:")
			mustWriteQueue(w, "    ", tableConfig.QueueConfig)
			if tableConfig.QueueConfig.Name != "" {

			}
		}
	}

	err = w.Flush()
	if err != nil {
		panic(err)
	}
}

func mustWriteQueue(w io.Writer, indent string, qc *SQSQueueConfig) {
	if qc.Name != "" {
		fmt.Fprintf(w, "%s  name: %s\n", indent, qc.Name)
	}

	if qc.Insert != nil {
		fmt.Fprintf(w, "%s  insert:\n", indent)
		mustWriteQueue(w, indent+"  ", qc.Insert)
	}

	if qc.Update != nil {
		fmt.Fprintf(w, "%s  update:\n", indent)
		mustWriteQueue(w, indent+"  ", qc.Update)
	}

	if qc.Delete != nil {
		fmt.Fprintf(w, "%s  delete:\n", indent)
		mustWriteQueue(w, indent+"  ", qc.Delete)
	}
}
