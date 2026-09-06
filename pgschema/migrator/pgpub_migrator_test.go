package migrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/pg2redis/redisconfig"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgCont "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

type testData struct {
	tName   string
	table   string
	schema  string
	pubName string
}

func TestPubCreatedWithAllColumnsV14(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(t.Context(),
		"docker.io/postgres:14",
		redisconfig.Prefix(),
		pgCont.WithInitScripts(filepath.Join("../../internal/integrationtest/testdata", "initpg.sql")),
		pgCont.WithInitScripts(filepath.Join("../../internal/integrationtest/testdata", "initpgv13.sql")),
	)
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(t.Context()) //nolint:errcheck

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(t.Context()))

	defer connector.Close(t.Context()) //nolint:errcheck

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(t.Context(), connStr)

	require.NoError(t, err)
	defer conn.Close(t.Context()) //nolint:errcheck

	test := cleanupAndPrepare(t, conn)

	mig := NewPublicationMigrator(l, connector, test.pubName)

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (id int not null primary key, txt text not null)", test.tName))

	tableConfig := map[string]*config.TableConfig{
		test.tName: {
			Columns: []string{config.AllColumns},
		},
	}

	err = mig.Migrate(t.Context(), tableConfig)
	require.NoError(t, err)

	ver, err := connector.PrimaryVersion()
	require.NoError(t, err)

	pubRes, err := pgschema.ReadPubTables(t.Context(), conn, test.pubName, ver)
	require.NoError(t, err)

	res := pubRes.Tables

	assert.Len(t, res, 1)
	assert.Equal(t, test.table, res[test.tName].Name.Name)
	assert.Equal(t, test.schema, res[test.tName].Name.Schema)
	// 2 columns
	assert.Len(t, res[test.tName].Columns, 2)
	assert.Equal(t, res[test.tName].Columns["id"].Name, "id")
	assert.True(t, res[test.tName].Columns["id"].IsPK)
	assert.Equal(t, res[test.tName].Columns["txt"].Name, "txt")
}

func TestPubCreatedWithAllColumnsV15(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(t.Context(),
		"docker.io/postgres:15",
		redisconfig.Prefix(),
		pgCont.WithInitScripts(filepath.Join("../../internal/integrationtest/testdata", "initpg.sql")),
		pgCont.WithInitScripts(filepath.Join("../../internal/integrationtest/testdata", "initpgv13.sql")),
	)
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(context.Background()) //nolint:errcheck

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	defer connector.Close(context.Background()) //nolint:errcheck

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer conn.Close(context.Background()) //nolint:errcheck

	test := cleanupAndPrepare(t, conn)

	mig := NewPublicationMigrator(l, connector, test.pubName)

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (id int not null primary key, txt text not null)", test.tName))

	tableConfig := map[string]*config.TableConfig{
		test.tName: {
			Columns: []string{config.AllColumns},
		},
	}

	err = mig.Migrate(context.Background(), tableConfig)
	require.NoError(t, err)

	res1, _, err := pgschema.ReadPubTablesV15(context.Background(), conn, test.pubName)
	require.NoError(t, err)
	assert.Len(t, res1, 1)
	assert.Equal(t, test.table, res1[test.tName].Name.Name)
	assert.Equal(t, test.schema, res1[test.tName].Name.Schema)
	// 2 columns
	assert.Len(t, res1[test.tName].Columns, 2)
	assert.Equal(t, res1[test.tName].Columns["id"].Name, "id")
	assert.True(t, res1[test.tName].Columns["id"].IsPK)
	assert.Equal(t, res1[test.tName].Columns["txt"].Name, "txt")
}

func TestPubCreatedWithOneColumnV15(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(context.Background(), "docker.io/postgres:15", redisconfig.Prefix())
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(context.Background()) //nolint:errcheck

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)

	require.NoError(t, err)
	require.NoError(t, connector.Connect(t.Context()))
	defer connector.Close(t.Context())

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(t.Context(), connStr)

	require.NoError(t, err)
	defer conn.Close(t.Context())

	test := cleanupAndPrepare(t, conn)

	mig := NewPublicationMigrator(l, connector, test.pubName)

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (id int not null primary key, txt text not null)", test.tName))

	tableConfig := map[string]*config.TableConfig{
		test.tName: {
			Columns: []string{"id"},
		},
	}

	err = mig.Migrate(t.Context(), tableConfig)
	require.NoError(t, err)

	res1, _, err := pgschema.ReadPubTablesV15(t.Context(), conn, test.pubName)

	require.NoError(t, err)
	assert.Len(t, res1, 1)
	assert.Equal(t, test.table, res1[test.tName].Name.Name)
	assert.Equal(t, test.schema, res1[test.tName].Name.Schema)
	assert.Len(t, res1[test.tName].Columns, 1)
	assert.Equal(t, res1[test.tName].Columns["id"].Name, "id")
}

func TestAlterPubAddV15(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(t.Context(), "docker.io/postgres:15", redisconfig.Prefix())
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(t.Context()) //nolint:errcheck

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	defer connector.Close(context.Background())

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	test := cleanupAndPrepare(t, conn)
	tName2 := fmt.Sprintf("%s_2", test.tName)

	mustExec(conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tName2))

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (col1 int not null primary key, col2 int not null, col3 int not null)", test.tName))
	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (col4 int not null primary key, col5 int not null)", tName2))
	tableConfig := map[string]*config.TableConfig{
		test.tName: &config.TableConfig{
			Columns: []string{"col1", "col2", "col3"},
		},
		tName2: {
			Columns: []string{"col4", "col5"},
		},
	}
	// we create publication without col3
	mustExec(conn, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s (col1, col2)", test.pubName, test.tName))

	mig := NewPublicationMigrator(l, connector, test.pubName)
	// Migrate must add col3 to the publication
	err = mig.Migrate(context.Background(), tableConfig)
	require.NoError(t, err)

	res1, _, err := pgschema.ReadPubTablesV15(context.Background(), conn, test.pubName)

	require.NoError(t, err)
	assert.Len(t, res1, 2)
	assert.Len(t, res1[test.tName].Columns, 3)
	assert.Len(t, res1[tName2].Columns, 2)
}

func TestAlterPubDropV15(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(context.Background(), "docker.io/postgres:15", redisconfig.Prefix())
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(context.Background())

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	defer connector.Close(context.Background())

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	test := cleanupAndPrepare(t, conn)
	tName2 := fmt.Sprintf("%s_2", test.tName)
	mustExec(conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tName2))

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (col1 int not null primary key, col2 int not null, col3 int not null)", test.tName))
	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (col4 int not null primary key, col5 int not null)", tName2))

	// we create publication with both tables
	mustExec(conn, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s", test.pubName, test.tName, tName2))

	// but config doesn't include 2nd table
	// as a result it should be dropped from publication
	tableConfig := map[string]*config.TableConfig{
		test.tName: &config.TableConfig{
			Columns: []string{"all"},
		},
	}

	mig := NewPublicationMigrator(l, connector, test.pubName)

	// Migrate must drop 2nd table from the publication
	err = mig.Migrate(context.Background(), tableConfig)
	require.NoError(t, err)

	res1, _, err := pgschema.ReadPubTablesV15(context.Background(), conn, test.pubName)
	require.NoError(t, err)

	assert.Len(t, res1, 1)
	assert.Equal(t, test.table, res1[test.tName].Name.Name)

	// all three columns should be in the publication
	assert.Len(t, res1[test.tName].Columns, 3)
}

func TestNoChangesWhenAllColumnsV15(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(context.Background(), "docker.io/postgres:15", redisconfig.Prefix())
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(context.Background())

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)
	require.NoError(t, err)
	require.NoError(t, connector.Connect(context.Background()))
	defer connector.Close(context.Background())

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	test := cleanupAndPrepare(t, conn)

	mig := NewPublicationMigrator(l, connector, test.pubName)

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (id int not null primary key, txt text not null)", test.tName))
	mustExec(conn, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", test.pubName, test.tName))

	tableConfig := map[string]*config.TableConfig{
		test.tName: &config.TableConfig{
			Columns: []string{"all"},
		},
	}

	pubTables, pubTablesWithAllColumns, err := pgschema.ReadPubTablesV15(t.Context(), conn, test.pubName)
	require.NoError(t, err)

	hasChanges := mig.hasChanges(pubTables, pubTablesWithAllColumns, tableConfig)

	assert.False(t, hasChanges)
}

func TestCreateReplicationSlotSQL(t *testing.T) {
	assert.Equal(t,
		"CREATE_REPLICATION_SLOT pg2redis_slot LOGICAL pgoutput EXPORT_SNAPSHOT",
		createReplicationSlotSQL("pg2redis_slot", false))

	assert.Equal(t,
		"CREATE_REPLICATION_SLOT pg2redis_slot LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER 'true')",
		createReplicationSlotSQL("pg2redis_slot", true))
}

func TestCreateReplicationSlotSQLWithFailoverIsAcceptedByPostgres17(t *testing.T) {
	if os.Getenv("SKIP_DOCKER_TESTS") != "" {
		t.Skip("Skipping Docker integration test")
	}

	ctx := t.Context()
	pgC, connOpts, err := integrationtest.CreatePGContainerOpts(ctx,
		"docker.io/postgres:17",
		"slotfailover",
		pgCont.WithInitScripts(filepath.Join("../../internal/integrationtest/testdata", "initpg.sql")),
	)
	require.NoError(t, err)
	defer pgC.Terminate(ctx) //nolint:errcheck

	connector, err := pgconnector.New(connOpts, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, connector.Connect(ctx))
	defer connector.Close(context.Background()) //nolint:errcheck

	replConn, err := connector.GetPrimaryReplConn(ctx)
	require.NoError(t, err)
	defer replConn.Close(context.Background()) //nolint:errcheck

	const slotName = "slot_failover_export_snapshot"
	result, err := pglogrepl.ParseCreateReplicationSlot(replConn.Exec(ctx, createReplicationSlotSQL(slotName, true)))
	require.NoError(t, err)

	assert.Equal(t, slotName, result.SlotName)
	assert.Equal(t, "pgoutput", result.OutputPlugin)
	assert.NotEmpty(t, result.ConsistentPoint)
	assert.NotEmpty(t, result.SnapshotName)
}

func TestMigrateSlotRejectsFailoverBeforePG17(t *testing.T) {
	failover := true
	mig := &PublicationMigrator{
		slotName:     "pg2redis_slot",
		slotFailover: &failover,
	}

	err := mig.migrateSlot(context.Background(), pgschema.V16)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSlotFailoverUnsupported)
}

func TestEffectiveSlotFailover(t *testing.T) {
	t.Run("defaults to false before PG17", func(t *testing.T) {
		mig := &PublicationMigrator{}

		failover, err := mig.effectiveSlotFailover(pgschema.V16)

		require.NoError(t, err)
		assert.False(t, failover)
	})

	t.Run("defaults to true on PG17 and newer", func(t *testing.T) {
		mig := &PublicationMigrator{}

		failover, err := mig.effectiveSlotFailover(pgschema.V17)

		require.NoError(t, err)
		assert.True(t, failover)
	})

	t.Run("allows explicit false on PG17", func(t *testing.T) {
		explicit := false
		mig := &PublicationMigrator{slotFailover: &explicit}

		failover, err := mig.effectiveSlotFailover(pgschema.V17)

		require.NoError(t, err)
		assert.False(t, failover)
	})

	t.Run("allows explicit true on PG17", func(t *testing.T) {
		explicit := true
		mig := &PublicationMigrator{slotFailover: &explicit}

		failover, err := mig.effectiveSlotFailover(pgschema.V17)

		require.NoError(t, err)
		assert.True(t, failover)
	})
}

func TestPubAllChangedToList(t *testing.T) {
	pgC, err := integrationtest.CreatePGContainer(context.Background(), "docker.io/postgres:15", redisconfig.Prefix())
	if err != nil {
		panic(err)
	}

	defer pgC.Terminate(context.Background())

	l := zap.NewNop()
	appConf, err := redisconfig.LoadAppConfig("")

	if err != nil {
		panic(err)
	}

	connector, err := pgconnector.New(appConf.Postgres.Conn, l)

	require.NoError(t, err)
	require.NoError(t, connector.Connect(t.Context()))

	defer connector.Close(t.Context())

	connStr := appConf.Postgres.Conn.AsOpt().CreateConnStr()
	conn, err := pgx.Connect(t.Context(), connStr)

	require.NoError(t, err)
	defer conn.Close(t.Context())

	test := cleanupAndPrepare(t, conn)

	mig := NewPublicationMigrator(l, connector, test.pubName)

	mustExec(conn, fmt.Sprintf("CREATE TABLE %s (id int not null primary key, txt text not null)", test.tName))
	// mustExec(conn, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s (id, txt)", test.pubName, test.tName))

	err = mig.Migrate(t.Context(), map[string]*config.TableConfig{
		test.tName: {
			Columns: []string{"all"},
		},
	})

	require.NoError(t, err)

	mustExec(conn, fmt.Sprintf("ALTER TABLE %s ADD new_col text", test.tName))
	err = mig.Migrate(t.Context(), map[string]*config.TableConfig{
		test.tName: {
			Columns: []string{"id, txt"},
		},
	})

	require.NoError(t, err)
	pubTables, pubTablesWithAllColumns, err := pgschema.ReadPubTablesV15(t.Context(), conn, test.pubName)
	require.NoError(t, err)
	assert.Len(t, pubTables, 1)
	assert.Len(t, pubTablesWithAllColumns, 1)
	// only id and txt
	assert.Len(t, pubTables[test.tName].Columns, 2)
	// all columns here
	assert.Len(t, pubTablesWithAllColumns[test.tName].Columns, 3)
}

func cleanupAndPrepare(t *testing.T, conn *pgx.Conn) testData {
	pubName := strings.ToLower(t.Name())
	table := pubName + "_table"
	schema := "test_schema"
	tName := fmt.Sprintf("%s.%s", schema, table)

	mustExec(conn, "CREATE SCHEMA IF NOT EXISTS test_schema")
	mustExec(conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
	mustExec(conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tName))

	return testData{
		tName:   tName,
		table:   table,
		schema:  schema,
		pubName: pubName,
	}
}

func mustExec(conn *pgx.Conn, sql string, args ...any) {
	_, err := conn.Exec(context.Background(), sql, args...)
	if err != nil {
		panic(err)
	}
}
