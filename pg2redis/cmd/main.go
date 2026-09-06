package main

import (
	"fmt"
	"os"

	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/pg2redis"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pg2runner"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	if os.Getenv("ENV_FILE") != "" {
		if err := godotenv.Load(os.Getenv("ENV_FILE")); err != nil {
			fmt.Printf("%v\n", err) //nolint:forbidigo
			os.Exit(-5)
		}
	}

	appConfPaths := []string{"/etc/pg2redis/config.yaml", "config.yaml"}
	cfgPath := os.Getenv("PG2REDIS_CONFIG_PATH")

	if cfgPath != "" {
		_, err := os.Stat(cfgPath)
		if err != nil {

			if os.IsNotExist(err) {
				fmt.Printf("config file %s does not exist\n", cfgPath)
			} else {
				fmt.Println("failed to open config file ", cfgPath, ":", err.Error())
			}

			os.Exit(-1)
		}

		appConfPaths = append(appConfPaths, cfgPath)
	}

	appConf, err := redisconfig.LoadAppConfig(appConfPaths...)
	if err != nil {
		fmt.Printf("failed to load app config: %v\n", err)
		os.Exit(-1)
	}

	logger, err := pglogger.CreateLogger(appConf.Debug, redisconfig.Prefix())
	if err != nil {
		// when we can't create logger - we print the error via fmt
		fmt.Printf("failed to init app logging: %v\n", err)
		os.Exit(-2)
	}

	defer logger.Sync()
	err = runApp(appConf, logger)
	if err != nil {
		logger.Sync()
		fmt.Println(err.Error())
		os.Exit(-3)
	}
}

func runApp(appConf *redisconfig.RedisAppConfig, logger *zap.Logger) error {
	return pg2runner.RunApp(appConf,
		redisconfig.Prefix(),
		licensemanager.ProductPG2REDIS,
		logger,
		func(appConf *redisconfig.RedisAppConfig, connector *pgconnector.PGConnector) (pg2runner.ListenerBuilder, error) {
			lc, err := pg2redis.NewListenerOptions(appConf)
			if err != nil {
				return nil, fmt.Errorf("failed to create redis listener: %w", err)
			}

			redisOpts, err := appConf.ReadRedisOptions(lc.WriterOpts.FlushOpts.Timeout)
			if err != nil {
				return nil, fmt.Errorf("failed to read redis options: %w", err)
			}

			return pg2redis.NewListener(redisOpts, *lc, connector, appConf, logger), nil
		})
}
