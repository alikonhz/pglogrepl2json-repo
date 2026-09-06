package pg2runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alikonhz/pglogrepl2json"
	"github.com/alikonhz/pglogrepl2json/appstate"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/appconfig"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/alikonhz/pglogrepl2json/licensemanager"
	"github.com/alikonhz/pglogrepl2json/pg2stats"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/migrator"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgcompatibility"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/replicationslot"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/alikonhz/pglogrepl2json/snapshot"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	_ "net/http/pprof"
)

type ListenerBuilder interface {
	LoadPubTables(ctx context.Context) error
	Ping(ctx context.Context) error
	Start(ctx context.Context)
	Listener() pglogrepl2json.ReplicationListener
	SnapshotListener(workers int) pglogrepl2json.SnapshotListener
	StatsCollector() pg2stats.StatLogger
	GetLastLSN(ctx context.Context, connection *pgconnector.PGConnector) (string, error)
	ValidateConfig(ctx context.Context) error
}

func RunApp[T appconfig.AppConfig](appConf T,
	prefix string,
	app licensemanager.Product,
	appLogger *zap.Logger,
	build func(appConf T, connector *pgconnector.PGConnector) (ListenerBuilder, error)) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := appLogger.Named("app-runner")
	if os.Getenv(fmt.Sprintf("%s_CPUPROF_ENABLED", strings.ToUpper(prefix))) == "true" {
		logger.Info("CPU profiling is enabled")

		// _ "net/http/pprof"
		// go tool pprof http://localhost:6060/debug/pprof/block
		// go tool pprof -http=:8080 http://localhost:6060/debug/pprof/profile
		//runtime.SetBlockProfileRate(1)
		//runtime.SetMutexProfileFraction(1)

		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()

		//f, err := os.Create("cpu.prof")
		//if err != nil {
		//	return fmt.Errorf("failed to create cpu.prof file. try disabling CPU profiling. error details: %w", err)
		//}
		//
		//defer f.Close()

		//runtime.SetCPUProfileRate(1000000 / 2)
		//if err := pprof.StartCPUProfile(f); err != nil {
		//	return fmt.Errorf("failed to start CPU profiling. try disabling CPU profiling. error details: %w", err)
		//}
		//
		//defer func() {
		//	pprof.StopCPUProfile()
		//	fmt.Println("CPU profiling stopped")
		//}()
	}

	pgConfig := appConf.PostgresConfig()
	err := pgConfig.Validate()

	if err != nil {
		return fmt.Errorf("invalid Postgres config: %w", err)
	}

	connector, err := pgconnector.NewWithApp(pgConfig.Conn, strings.ToLower(string(app)), logger)
	if err != nil {
		return fmt.Errorf("failed to validate Postgres connection parameters: %w", err)
	}

	_, err = connector.ConnectPrimaryNode(ctx)
	if err != nil {
		return err
	}

	defer connector.Close(context.Background())

	today, err := getToday(connector)
	if err != nil {
		return fmt.Errorf("failed to get current date: %w", err)
	}

	logger.Info("today", zap.String("today", today.String()))

	err = licensemanager.VerifyAndSetLicenseFromEnv(app, today)

	if err != nil {
		return fmt.Errorf("license check failed: %w", err)
	}

	logger.Info("license is valid", zap.String("valid_until", licensemanager.CurrentLicense().End.AsDate().Format(time.DateOnly)))

	migration, err := migrateAndValidate(ctx, appConf, connector, logger)

	if err != nil {
		return err
	}

	flushLSN := migration.flushLSN
	slotSnapshot := migration.slotSnapshot
	defer func() {
		closeSlotSnapshot(logger, slotSnapshot)
	}()

	var (
		standByTimeout  time.Duration
		receiveTimeout  time.Duration
		statsInterval   time.Duration
		shutdownTimeout time.Duration
	)

	pgWalSenderTimeout := readPgWalSenderTimeout(ctx, connector, logger)

	if pgConfig.StandByTimeout != "" {
		standByTimeout, err = config.DurationOrSeconds(pgConfig.StandByTimeout)
		if err != nil {
			return fmt.Errorf("invalid standByTimeout value %q: %w", pgConfig.StandByTimeout, err)
		}

		if standByTimeout > pgWalSenderTimeout {
			logger.Warn("standByTimeout is greater than wal_sender_timeout. App will use wal_sender_timeout as standByTimeout")

			standByTimeout = pgWalSenderTimeout
		}
	} else {
		standByTimeout = pgWalSenderTimeout
	}

	if pgConfig.ReceiveTimeout != "" {
		receiveTimeout, err = config.DurationOrSeconds(pgConfig.ReceiveTimeout)
		if err != nil {
			return fmt.Errorf("invalid receiveTimeout value %q: %w", pgConfig.ReceiveTimeout, err)
		}
	} else {
		receiveTimeout = 60 * time.Second //nolint:mnd, default
	}

	appSettings := appConf.Settings()

	if appSettings.StatsInterval != "" {
		si, err := config.DurationOrSeconds(appSettings.StatsInterval)
		if err != nil {
			return fmt.Errorf("invalid statsInterval value %q: %w", appSettings.StatsInterval, err)
		}

		statsInterval = si
	} else {
		statsInterval = time.Minute // default
	}

	if appSettings.ShutdownTimeout != "" {
		st, err := config.DurationOrMilliseconds(appSettings.ShutdownTimeout)
		if err != nil {
			return fmt.Errorf("invalid shutdownTimeout value %q: %w", appSettings.ShutdownTimeout, err)
		}

		shutdownTimeout = st
	} else {
		shutdownTimeout = 0
	}

	maxWriteQueueSize := appSettings.MaxWriteQueueSize
	opts := replicator.NewOptions(pgConfig.Repl.Slot, pgConfig.Repl.Pub, standByTimeout, receiveTimeout, statsInterval, maxWriteQueueSize)

	logger.Debug("starting", zap.Duration("standByTimeout", opts.StandByTimeout),
		zap.Duration("receiveTimeout", opts.ReceiveMsgTimeout),
		zap.Uint64("maxWriteQueueSize", opts.MaxWriteQueueSize))

	setWatchXID(prefix, &opts)

	if opts.WatchXID > 0 {
		logger.Sugar().Info("WATCHXID = ", opts.WatchXID)
	}

	listenerBuilder, err := build(appConf, connector)
	if err != nil {
		return err
	}

	err = listenerBuilder.LoadPubTables(ctx)
	if err != nil {
		return err
	}

	err = listenerBuilder.ValidateConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}
	logger.Info("connecting to downstream...")
	err = listenerBuilder.Ping(ctx)
	if err != nil {
		return err
	}

	// The snapshot is executed before streaming starts
	snapshotLsn := flushLSN
	snapshotCfg := appConf.SnapshotConfig()

	if snapshotCfg != nil && snapshotCfg.Mode != appconfig.ModeNever {
		appConf.NeedsTrackCommitTimestamp()
		snapMgr := snapshot.NewManager(snapshotCfg, connector, listenerBuilder.SnapshotListener(snapshotCfg.ParallelWorkers), string(app), appLogger).
			WithSlotSnapshot(slotSnapshot).
			WithStatsInterval(statsInterval)
		snapLsn, err := snapMgr.Execute(ctx)
		if err != nil {
			logger.Error("snapshot failed", zap.Error(err))
			if snapshotCfg.AbortOnError {
				return fmt.Errorf("snapshot failed: %w", err)
			}
		} else if snapLsn > 0 {
			snapshotLsn = snapLsn
		}

		if snapshotCfg.Mode == appconfig.ModeOneTimeOnly {
			logger.Info("snapshot mode is 'onetime_only', shutting down.")
			return nil
		}
	}

	closeSlotSnapshot(logger, slotSnapshot)
	slotSnapshot = nil

	repl := replicator.MustCreateWithLogger(opts, listenerBuilder.Listener(), listenerBuilder.StatsCollector(), connector, string(app), appLogger)
	doneChan := make(chan error)

	listenerBuilder.Start(ctx)
	logger.Info("starting replication...")

	err = appstate.Create(ctx, connector)
	if err != nil {
		logger.Warn("failed to create app state", zap.Error(err))
	}

	startLSN, err := listenerBuilder.GetLastLSN(ctx, connector)
	var pgStartLSN pglogrepl.LSN

	if err != nil || startLSN == "" {
		if err != nil {
			logger.Warn("failed to read LSN", zap.Error(err))
		}

		pgStartLSN = snapshotLsn
	} else {
		pgStartLSN, err = pglogrepl.ParseLSN(startLSN)
		if err != nil {
			logger.Warn("failed to parse LSN", zap.Error(err), zap.String(pglogger.LSNParam, startLSN))

			pgStartLSN = snapshotLsn
		} else if pgStartLSN == 0 {
			pgStartLSN = snapshotLsn
		}
	}

	logger.Info("start LSN", zap.String(pglogger.LSNParam, pgStartLSN.String()))

	err = repl.StartFromLsn(ctx, pgStartLSN, doneChan)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	shutdownFunc := func() {
		var shutdownCtx context.Context

		if shutdownTimeout > 0 {
			sc, scCancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer scCancel()

			shutdownCtx = sc
		} else {
			shutdownCtx = context.Background()
		}

		repl.GracefulShutdown(shutdownCtx)
	}

	logger.Info("replication started")

	select {
	case <-ctx.Done():
		shutdownFunc()

		break

	case err = <-doneChan:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("app exited with error: %w", err)
		}
	}

	return nil
}

func getToday(connector *pgconnector.PGConnector) (time.Time, error) {
	pgToday, err := pgconnector.ExecPrimary[time.Time](context.Background(), connector, func(ctx context.Context, conn *pgx.Conn) (time.Time, error) {
		row := conn.QueryRow(ctx, "select CURRENT_DATE")
		var today time.Time
		err := row.Scan(&today)
		return today, err
	})

	if err != nil {
		return time.Time{}, err
	}

	now := time.Now()
	year, month, day := now.Date()
	location := now.Location()

	appToday := time.Date(year, month, day, 0, 0, 0, 0, location)

	if pgToday.After(appToday) {
		return pgToday, nil
	}

	return appToday, nil
}

func closeSlotSnapshot(logger *zap.Logger, slotSnapshot *replicationslot.Snapshot) {
	if slotSnapshot == nil {
		return
	}

	if err := slotSnapshot.Close(context.Background()); err != nil {
		logger.Warn("failed to close replication slot snapshot", zap.Error(err))
	}
}

func readPgWalSenderTimeout(ctx context.Context, connector *pgconnector.PGConnector, logger *zap.Logger) time.Duration {
	walSenderTimeout, err := pgconnector.ExecPrimary(ctx, connector, func(ctx context.Context, conn *pgx.Conn) (string, error) {
		r := conn.QueryRow(ctx, `
SELECT setting::int * CASE unit
    WHEN 'ms' THEN 1
    WHEN 's' THEN 1000
    WHEN 'min' THEN 60000
    WHEN 'h' THEN 3600000
    ELSE 1
END AS wal_sender_timeout_ms
FROM pg_catalog.pg_settings
WHERE name = 'wal_sender_timeout'`)

		var value string
		err := r.Scan(&value)
		if err != nil {
			return "", err
		}

		return value, nil
	})

	if err != nil {
		logger.Warn("failed to get default standby timeout", zap.Error(err))
		return 10 * time.Second
	}

	value, err := config.DurationOrMilliseconds(walSenderTimeout)
	if err != nil {
		logger.Warn("failed to parse wal_sender_timeout", zap.Error(err), zap.String("wal_sender_timeout", walSenderTimeout))
		return 10 * time.Second
	}

	return value
}

func validateWithConn(ctx context.Context, conf appconfig.AppConfig, conn *pgx.Conn, logger *zap.Logger) (pglogrepl.LSN, error) {
	var (
		err      error
		flushLSN pglogrepl.LSN
	)

	logger.Debug("loading publication info from Postgres...")

	flushLSN, err = createConnStrAndLoadPublication(ctx, conn, conf.PostgresConfig().Repl)
	if err != nil {
		return 0, fmt.Errorf("failed to load publication config: %w", err)
	}

	ver, err := pgschema.ReadPGVersion(ctx, conn)
	if err != nil {
		return 0, fmt.Errorf("failed to read PG version: %w", err)
	}

	pubRes, err := pgschema.ReadPubTables(ctx, conn, conf.PostgresConfig().Repl.Pub, ver)
	if err != nil {
		return 0, fmt.Errorf("failed to read publication tables: %w", err)
	}

	if err := validateSchema(pubRes, conf.TablesConfig(), conf.PostgresConfig().Repl.Pub); err != nil {
		return 0, err
	}

	logger.Debug("done loading publication info from Postgres")

	if conf.NeedsTrackCommitTimestamp() {
		logger.Debug("checking if track_commit_timestamp is enabled")

		err = pgcompatibility.CheckTrackCommitTimestamp(ctx, conn)

		if err != nil {
			if errors.Is(err, pgcompatibility.ErrTrackCommitTimestampDisabled) {
				return 0, fmt.Errorf("%w: it must be enabled in order to use commitTimeColumn option", err)
			}

			return 0, fmt.Errorf("failed to check track_commit_timestamp: %w", err)
		}

		logger.Debug("done checking if track_commit_timestamp is enabled")
	}

	return flushLSN, nil
}

type migrationResult struct {
	flushLSN     pglogrepl.LSN
	slotSnapshot *replicationslot.Snapshot
}

func migrateAndValidate(ctx context.Context, conf appconfig.AppConfig, connector *pgconnector.PGConnector, logger *zap.Logger) (migrationResult, error) {
	result := migrationResult{}

	if strings.EqualFold(string(conf.PostgresConfig().Repl.Owner), "app") {
		logger.Debug("replication owner is 'app', performing migration...")
		replConfig := conf.PostgresConfig().Repl
		mig := migrator.NewPublicationMigrator(logger, connector, replConfig.Pub, migrator.ReplicationSlotConfig{
			Name:     replConfig.Slot,
			Failover: replConfig.Failover,
		})
		err := mig.Migrate(ctx, conf.TablesConfig())
		if err != nil {
			return result, fmt.Errorf("failed to migrate publication: %w", err)
		}

		result.slotSnapshot = mig.CreatedSlotSnapshot()
	} else {
		logger.Debug("replication owner is 'user', skipping migration...")
	}

	flushLSN, err := pgconnector.ExecPrimary(ctx, connector, func(ctx context.Context, conn *pgx.Conn) (pglogrepl.LSN, error) {
		return validateWithConn(ctx, conf, conn, logger)
	})

	if err != nil {
		closeSlotSnapshot(logger, result.slotSnapshot)
		return result, err
	}

	if err = validateCreatedSlotLSN(result.slotSnapshot, flushLSN); err != nil {
		closeSlotSnapshot(logger, result.slotSnapshot)
		return result, err
	}

	result.flushLSN = flushLSN

	return result, nil
}

func validateCreatedSlotLSN(slotSnapshot *replicationslot.Snapshot, confirmedFlushLSN pglogrepl.LSN) error {
	if slotSnapshot == nil {
		return nil
	}

	if slotSnapshot.LSN != confirmedFlushLSN {
		return fmt.Errorf("created replication slot LSN %s does not match confirmed_flush_lsn %s",
			slotSnapshot.LSN.String(),
			confirmedFlushLSN.String())
	}

	return nil
}

func setWatchXID(prefix string, o *replicator.Options) {
	watchXID := os.Getenv(fmt.Sprintf("%s_WATCHXID", strings.ToUpper(prefix)))
	if watchXID == "" {
		return
	}

	xid, err := strconv.ParseUint(watchXID, 10, 32)
	if err != nil {
		return
	}

	o.WatchXID = uint32(xid)
}

func validateSchema(pubRes pgschema.PubTableRes, tablesConfig map[string]*config.TableConfig, publication string) error {
	for name, tableConf := range tablesConfig {
		table, ok := pubRes.Tables[name]
		if !ok {
			// If not in publication explicitly (maybe via custom column list), check if it's there at all
			table, ok = pubRes.TablesWithAllColumns[name]
			if !ok {
				return fmt.Errorf("table %s is not in publication %s", name, publication)
			}
		}

		for _, col := range tableConf.Columns {
			if col == config.AllColumns {
				continue
			}
			if !table.HasColumn(col) {
				return fmt.Errorf("column %q for table %q is not in publication %q", col, name, publication)
			}
		}
	}
	return nil
}

func createConnStrAndLoadPublication(ctx context.Context, conn *pgx.Conn, pgSlotConfig pgconfig.PgReplConfig) (pglogrepl.LSN, error) {
	row := conn.QueryRow(ctx,
		"select pubname from pg_publication where pubname = $1",
		pgSlotConfig.Pub)
	var pubName string
	err := row.Scan(&pubName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("publication %s doesn't exist", pgSlotConfig.Pub)
		}
	}

	//create publication pg2redis_publication for table orders, customers
	//select pg_create_logical_replication_slot('pg2redis_slot', 'test_decoding')
	//select pg_create_logical_replication_slot('pg2redis_slot', 'pgoutput')

	row = conn.QueryRow(ctx,
		"select slot_name, plugin, confirmed_flush_lsn from pg_replication_slots where slot_name = $1 and database = current_database()",
		pgSlotConfig.Slot,
	)
	var slotName string
	var plugin string
	var flushLSN pglogrepl.LSN
	err = row.Scan(&slotName, &plugin, &flushLSN)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("slot %s doesn't exist", pgSlotConfig.Slot)
		}

		return 0, err
	}

	if plugin == "" || !strings.EqualFold(plugin, "pgoutput") {
		return 0, fmt.Errorf("slot %s must be created with 'pgoutput' plugin (current plugin is %q)",
			pgSlotConfig.Slot,
			plugin)
	}

	return flushLSN, nil
}
