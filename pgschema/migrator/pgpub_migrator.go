package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/pglogger"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/replicationslot"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

var (
	UnsupportedPrimaryVersionError = errors.New("unsupported primary node version")
	ErrSlotFailoverUnsupported     = errors.New("replication slot failover is supported only in Postgres version >= 17")
)

type ReplicationSlotConfig struct {
	Name     string
	Failover *bool
}

type PublicationMigrator struct {
	pubName             string
	slotName            string
	slotFailover        *bool
	connector           *pgconnector.PGConnector
	logger              *zap.Logger
	createdSlotSnapshot *replicationslot.Snapshot
}

func NewPublicationMigrator(logger *zap.Logger, connector *pgconnector.PGConnector, pubName string, slotConfig ...ReplicationSlotConfig) *PublicationMigrator {
	var configuredSlot ReplicationSlotConfig
	if len(slotConfig) > 0 {
		configuredSlot = slotConfig[0]
	}

	return &PublicationMigrator{
		pubName:      pubName,
		slotName:     configuredSlot.Name,
		slotFailover: configuredSlot.Failover,
		logger:       logger,
		connector:    connector,
	}
}

func (pm *PublicationMigrator) CreatedSlotSnapshot() *replicationslot.Snapshot {
	return pm.createdSlotSnapshot
}

func (pm *PublicationMigrator) Migrate(ctx context.Context, tableConfig map[string]*config.TableConfig) error {
	pm.createdSlotSnapshot = nil

	primVersion, err := pm.connector.PrimaryVersion()
	if err != nil {
		return fmt.Errorf("%w %d", UnsupportedPrimaryVersionError, primVersion)
	}

	_, err = pgconnector.ExecPrimary(ctx, pm.connector, func(ctx context.Context, conn *pgx.Conn) (struct{}, error) {
		return pm.migrateWithConn(ctx, conn, primVersion, tableConfig)
	})

	if err != nil {
		return fmt.Errorf("migration error: %w", err)
	}

	if pm.slotName != "" {
		if err = pm.migrateSlot(ctx, primVersion); err != nil {
			return fmt.Errorf("slot migration error: %w", err)
		}
	}

	return nil
}

func (pm *PublicationMigrator) migrateSlot(ctx context.Context, version pgschema.PGVersion) error {
	slotFailover, err := pm.effectiveSlotFailover(version)
	if err != nil {
		return err
	}

	slotExists, err := pgconnector.ExecPrimary(ctx, pm.connector, func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		return pm.slotExists(ctx, conn)
	})
	if err != nil {
		return fmt.Errorf("failed to check replication slot: %w", err)
	}

	if slotExists {
		pm.logger.Debug("replication slot already exists", zap.String("slot", pm.slotName))
		return nil
	}

	return pm.createSlot(ctx, slotFailover)
}

func (pm *PublicationMigrator) effectiveSlotFailover(version pgschema.PGVersion) (bool, error) {
	if pm.slotFailover == nil {
		return version >= pgschema.V17, nil
	}

	if *pm.slotFailover && version < pgschema.V17 {
		return false, fmt.Errorf("%w: primary version %d", ErrSlotFailoverUnsupported, version)
	}

	return *pm.slotFailover, nil
}

func (pm *PublicationMigrator) slotExists(ctx context.Context, conn *pgx.Conn) (bool, error) {
	row := conn.QueryRow(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM pg_replication_slots
	WHERE slot_name = $1
	AND database = current_database()
)`, pm.slotName)

	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, err
	}

	return exists, nil
}

func (pm *PublicationMigrator) createSlot(ctx context.Context, failover bool) error {
	pm.logger.Debug("creating new replication slot", zap.String("slot", pm.slotName))

	replConn, err := pm.connector.GetPrimaryReplConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect via replication connection: %w", err)
	}

	result, err := pglogrepl.ParseCreateReplicationSlot(replConn.Exec(ctx, pm.createReplicationSlotSQL(failover)))
	if err != nil {
		_ = replConn.Close(context.Background())
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		_ = replConn.Close(context.Background())
		return fmt.Errorf("failed to parse replication slot consistent point: %w", err)
	}

	if result.SnapshotName == "" {
		_ = replConn.Close(context.Background())
		return errors.New("replication slot was created without an exported snapshot name")
	}

	pm.createdSlotSnapshot = replicationslot.NewSnapshot(lsn, result.SnapshotName, replConn.Close)
	pm.logger.Debug("created replication slot",
		zap.String("slot", result.SlotName),
		zap.String(pglogger.LSNParam, lsn.String()),
		zap.String("snapshot_id", result.SnapshotName))

	return nil
}

func (pm *PublicationMigrator) createReplicationSlotSQL(failover bool) string {
	return createReplicationSlotSQL(pm.slotName, failover)
}

func createReplicationSlotSQL(slotName string, failover bool) string {
	if failover {
		return fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER 'true')", slotName)
	}

	return fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput EXPORT_SNAPSHOT", slotName)
}

func (pm *PublicationMigrator) migrateWithConn(ctx context.Context, conn *pgx.Conn, version pgschema.PGVersion, tableConfig map[string]*config.TableConfig) (struct{}, error) {
	res, err := pgschema.ReadPubTables(ctx, conn, pm.pubName, version)
	if err != nil && !errors.Is(err, pgschema.ErrPubNotFound) {
		return struct{}{}, fmt.Errorf("failed to read pub tables: %w", err)
	}

	if errors.Is(err, pgschema.ErrPubNotFound) {
		pm.logger.Debug("creating new publication", zap.String(pglogger.PubNameParam, pm.pubName))

		return struct{}{}, pm.createPub(ctx, conn, version, tableConfig)
	}

	pm.logger.Debug("updating publication", zap.String(pglogger.PubNameParam, pm.pubName))

	// difference between columns: all and columns: [list of columns (possibly all)]
	// when we set columns: all -> newly added columns will be returned by pgoutput automatically
	// when we set columns: [<list of columns>] -> new columns won't be possible returned by pgoutput (TODO: check this)
	hasChanges := pm.hasChanges(res.Tables, res.TablesWithAllColumns, tableConfig)
	if !hasChanges {
		pm.logger.Debug("no publication changes detected", zap.String(pglogger.PubNameParam, pm.pubName))
		return struct{}{}, nil
	}

	if err = pm.alterPubSet(ctx, conn, tableConfig); err != nil {
		return struct{}{}, err
	}

	return struct{}{}, nil
}

func (pm *PublicationMigrator) hasChanges(pubTables map[string]*pgschema.Table,
	pubTablesWithAllColumns map[string]*pgschema.Table,
	tableConfig map[string]*config.TableConfig) bool {
	for tableName, tableInConfig := range tableConfig {
		pubTable, ok := pubTables[tableName]
		if !ok {
			return true
		}

		pubTableWithAllColumn, ok := pubTablesWithAllColumns[tableName]
		if !ok {
			return true
		}

		if pm.columnsDiffer(pubTable, pubTableWithAllColumn, tableInConfig) {
			return true
		}
	}

	for k := range pubTables {
		if _, ok := tableConfig[k]; !ok {
			return true
		}
	}

	return false
}

func (pm *PublicationMigrator) columnsDiffer(pubTable *pgschema.Table,
	pubTableWithAllColumns *pgschema.Table,
	configTable *config.TableConfig) bool {
	if configTable.AllColumns() {
		return columnsDifferWithAllInConfig(pubTable, pubTableWithAllColumns)
	}

	if len(pubTable.Columns) != len(configTable.Columns) {
		return true
	}

	for _, column := range configTable.Columns {
		if _, ok := pubTable.Columns[column]; !ok {
			return true
		}
	}

	return false
}

func columnsDifferWithAllInConfig(pubTable *pgschema.Table, pubTableWithAllColumns *pgschema.Table) bool {
	if len(pubTable.Columns) != len(pubTableWithAllColumns.Columns) {
		return true
	}

	for _, col1 := range pubTable.Columns {
		if _, ok := pubTableWithAllColumns.Columns[col1.Name]; !ok {
			return true
		}
	}

	return false
}

func (pm *PublicationMigrator) alterPubSet(ctx context.Context, conn *pgx.Conn, tables map[string]*config.TableConfig) error {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("ALTER PUBLICATION %s SET TABLE\n", pm.pubName))
	createPubString(&str, tables)
	s := str.String()

	if l := pm.logger.Check(zap.DebugLevel, "alter publication"); l != nil {
		l.Write(zap.String(pglogger.PubNameParam, pm.pubName), zap.String("alter_sql", s))
	}

	if _, err := conn.Exec(ctx, str.String()); err != nil {
		return fmt.Errorf("alter publication error: %w", err)
	}

	return nil
}

func (pm *PublicationMigrator) createPub(ctx context.Context, conn *pgx.Conn, pgVersion pgschema.PGVersion, tableConfig map[string]*config.TableConfig) error {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE\n", pm.pubName))
	createPubString(&str, tableConfig)
	if pgVersion >= pgschema.V13 {
		str.WriteString(" WITH(publish_via_partition_root=true) ")
	}

	s := str.String()
	if l := pm.logger.Check(zap.DebugLevel, "create publication"); l != nil {
		l.Write(zap.String(pglogger.PubNameParam, pm.pubName), zap.String("create_sql", s))
	}

	_, err := conn.Exec(ctx, s)

	if err != nil {
		return fmt.Errorf("create publication error: %w", err)
	}

	return nil
}

func createPubString(str *strings.Builder, tableConfig map[string]*config.TableConfig) {
	comma := ""

	for k, v := range tableConfig {
		var columns string
		if !v.AllColumns() && len(v.Columns) > 0 {
			columns = "(" + strings.Join(v.Columns, ",") + ")"
		}

		str.WriteString(fmt.Sprintf("%s %s %s", comma, k, columns))
		comma = ","
	}
}
