package appconfig

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type Builder interface {
	CreateEmptyConfig() any

	Prefix() string
	WellForm(env any)
	Validate() error
}

type AppSettings struct {
	StatsInterval     string
	MaxWriteQueueSize uint64
	ShutdownTimeout   string
}

type FlushOptions struct {
	Interval   time.Duration
	BufferSize uint32
	QueueDepth uint32
	Workers    uint32
	Timeout    time.Duration
}

const (
	maxFlushQueueDepth = 32
)

func NewFlushOptions(interval string, bufferSize uint32, queueDepth uint32, workers uint32, timeout string) (FlushOptions, error) {
	var (
		flushInterval   time.Duration
		timeoutDuration time.Duration
	)

	if queueDepth == 0 || queueDepth > maxFlushQueueDepth {
		queueDepth = maxFlushQueueDepth
	}

	if interval != "" {
		fi, err := config.DurationOrMilliseconds(interval)
		if err != nil {
			return FlushOptions{}, fmt.Errorf("invalid flushInterval: %w", err)
		}

		flushInterval = fi
	} else {
		const defaultInterval = 500 * time.Millisecond
		flushInterval = defaultInterval
	}

	if workers == 0 {
		maxProc := runtime.GOMAXPROCS(0)

		var idealNumber uint32

		if maxProc < 1 {
			idealNumber = 1
		} else if maxProc > 32 {
			idealNumber = 32
		} else {
			idealNumber = uint32(maxProc)
		}

		workers = idealNumber
	}

	if timeout != "" {
		writeTimeout, err := config.DurationOrSeconds(timeout)
		if err != nil {
			return FlushOptions{}, fmt.Errorf("invalid writeTimeout: %w", err)
		}

		timeoutDuration = writeTimeout
	} else {
		const defaultWriteTimeout = 10 * time.Second
		timeoutDuration = defaultWriteTimeout
	}

	return FlushOptions{
			Interval:   flushInterval,
			BufferSize: bufferSize,
			QueueDepth: queueDepth,
			Workers:    workers,
			Timeout:    timeoutDuration,
		},
		nil
}

type TxCommitTimeOptions struct {
	Save bool
	Name string
}

func NewTxCommitTimeOptions(commitTimePropName string) TxCommitTimeOptions {
	txCommitCol := strings.TrimSpace(commitTimePropName)
	save := txCommitCol != ""
	return TxCommitTimeOptions{Save: save, Name: txCommitCol}
}

type AppConfig interface {
	PostgresConfig() pgconfig.PgConfig
	TablesConfig() map[string]*config.TableConfig
	NeedsTrackCommitTimestamp() bool
	Settings() AppSettings
	RetryPolicy() RetryPolicy
	SnapshotConfig() *SnapshotConfig
}

func LoadAppConfig(builder Builder, configPath ...string) error {
	return doLoadAppConfig(builder, true, configPath...)
}

func LoadAppConfigNoEnv(builder Builder, configPath ...string) error {
	return doLoadAppConfig(builder, false, configPath...)
}

func doLoadAppConfig(builder Builder, loadEnv bool, configPath ...string) error {
	k := koanf.New(".")
	var allConfigPath []string
	if len(configPath) == 0 {
		allConfigPath = append(allConfigPath, "config.yaml")
	} else {
		allConfigPath = configPath
	}

	appConf := builder.CreateEmptyConfig()
	for _, cp := range allConfigPath {
		if cp == "" {
			continue
		}

		if err := k.Load(file.Provider(cp), yaml.Parser()); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}

	if loadEnv {
		if err := k.Load(env.Provider(builder.Prefix()+"_", ".", func(s string) string {
			return strings.Replace(strings.ToLower(strings.TrimPrefix(s, builder.Prefix()+"_")), "_", ".", -1)
		}), nil); err != nil {
			return err
		}
	}

	if err := k.UnmarshalWithConf("", &appConf, koanf.UnmarshalConf{Tag: "json"}); err != nil {
		return err
	}

	builder.WellForm(k.Get("t"))

	if err := builder.Validate(); err != nil {
		return err
	}

	return nil
}

func WellFormTableName(key string) string {
	newKey := key
	if !strings.Contains(key, ".") {
		newKey = fmt.Sprintf("public.%s", key)
	}

	return newKey
}

func WellFormPgConfig(config pgconfig.PgConfig) pgconfig.PgConfig {
	if config.NumericMode != "" {
		return config
	}

	config.NumericMode = pgconfig.PgNumericModeFloat
	return config
}
