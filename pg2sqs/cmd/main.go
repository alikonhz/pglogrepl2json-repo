package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/pg2runner"
	"github.com/alikonhz/pglogrepl2json/pg2sqs"
	"github.com/alikonhz/pglogrepl2json/pg2sqs/sqsconfig"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

//func printGoroutines() {
//	buf := make([]byte, 1<<16)
//	runtime.Stack(buf, true)
//	fmt.Printf("%s\n", buf)
//}

func main() {
	if os.Getenv("ENV_FILE") != "" {
		if err := godotenv.Load(os.Getenv("ENV_FILE")); err != nil {
			fmt.Printf("%v\n", err) //nolint:forbidigo
			os.Exit(-5)
		}
	}

	appConfPaths := []string{"/etc/opt/pg2sqs/config.yaml", "config.yaml"}
	cfgPath := os.Getenv("PG2SQS_CONFIG_PATH")

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

	appConf, err := sqsconfig.LoadAppConfig(appConfPaths...)

	if err != nil {
		fmt.Printf("failed to load app config: %v\n", err) //nolint:forbidigo
		os.Exit(-1)
	}

	_, err = appConf.RetryPolicy().AsRetryConfig()

	if err != nil {
		fmt.Printf("retry config is invalid: %v\n", err) //nolint:forbidigo
		os.Exit(-2)
	}

	logger, err := pglogger.CreateLogger(appConf.Debug, sqsconfig.Prefix())

	if err != nil {
		// when we can't create logger - we print the error via fmt
		fmt.Printf("failed to init app logging: %v\n", err) //nolint:forbidigo
		os.Exit(-3)
	}

	defer logger.Sync() //nolint:errcheck

	err = runApp(appConf, logger)
	if err != nil {
		logger.Sync()            //nolint:errcheck
		fmt.Println(err.Error()) //nolint:forbidigo
		os.Exit(-4)              //nolint:gocritic
	}
}

func runApp(appConf *sqsconfig.SQSAppConfig, logger *zap.Logger) error {
	return pg2runner.RunApp(appConf, //nolint:wrapcheck
		sqsconfig.Prefix(),
		licensemanager.ProductPG2SQS,
		logger,
		func(appConf *sqsconfig.SQSAppConfig, connector *pgconnector.PGConnector) (pg2runner.ListenerBuilder, error) {
			lc, err := pg2sqs.NewListenerConfig(appConf)
			if err != nil {
				return nil, err
			}

			cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRetryer(func() aws.Retryer {
				return aws.NopRetryer{}
			}))

			if err != nil {
				return nil, fmt.Errorf("sqs config load error: %w", err)
			}

			sqsClient := sqs.NewFromConfig(cfg)

			return pg2sqs.NewListener(sqsClient, lc, connector, appConf, logger), nil
		})
}
