package integrationtest

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/config/pgconfig"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	EnvPgTestConn = "PG_TEST_CONN"
)

type Table string
type Slot string
type Pub string

type PGTestOptions struct {
	Table   string
	Slot    string
	Pub     string
	Timeout time.Duration
	Opts    *config.ConnectionOptsMany
}

type PGTestMultiOptions struct {
	Tables  []string
	Slot    string
	Pub     string
	Timeout time.Duration
	Opts    *config.ConnectionOptsMany
}

func (t PGTestOptions) TableKey() string {
	return fmt.Sprintf("public.%s", t.Table)
}

func MustCreateMultiTestOptionsWithConfig(ctx context.Context, pgConfig pgconfig.PgConfig, provider TableNameProvider) PGTestMultiOptions {
	t, s, p := MustCreateTablesSlotAndPubWithProviderAndConfig(ctx, pgConfig, pgConfig.Repl, provider)
	timeout := 10 * time.Second

	return PGTestMultiOptions{
		Tables:  t,
		Slot:    s,
		Pub:     p,
		Timeout: timeout,
		Opts:    pgConfig.Conn,
	}
}

func MustCreateTestOptionsWithConfig(ctx context.Context, pgConfig pgconfig.PgConfig) PGTestOptions {
	t, s, p := MustCreateTableSlotAndPubWithConfig(ctx, pgConfig, pgConfig.Repl)
	timeout := 10 * time.Second

	return PGTestOptions{
		Table:   string(t),
		Slot:    string(s),
		Pub:     string(p),
		Opts:    pgConfig.Conn,
		Timeout: timeout,
	}
}

func MustCreateTestOptions(ctx context.Context, slotPrefix string) PGTestOptions {
	t, s, p := MustCreateTableSlotAndPub(ctx, slotPrefix)
	opts := MustCreateReplConnectionFromEnv()
	timeout := 10 * time.Second

	return PGTestOptions{
		Table:   string(t),
		Slot:    string(s),
		Pub:     string(p),
		Opts:    opts,
		Timeout: timeout,
	}
}

func MustLoad(fileName string) {
	fmt.Println("reading ", fileName)
	err := godotenv.Load(fileName)
	if err != nil {
		panic(err)
	}
}

func MustLogicalDecodingWorkMemWithConfig(pgConfig pgconfig.PgConfig) {
	p := MustCreateDbConnectionFromConfig(pgConfig)
	defer p.Close()
	mustEnsureLogicalDecodingWorkMem(p)
}

func MustLogicalDecodingWorkMem(ctx context.Context) {
	p := MustCreateDbConnectionFromEnv(ctx)
	defer p.Close()
	mustEnsureLogicalDecodingWorkMem(p)
}

func mustEnsureLogicalDecodingWorkMem(p *pgxpool.Pool) {
	rows, err := p.Query(context.Background(), "select current_setting ('logical_decoding_work_mem', true)")
	if err != nil {
		panic(fmt.Errorf("unable to read logical_decoding_work_mem config: %w", err))
	}

	defer rows.Close()
	if !rows.Next() {
		panic("no result returned for logical_decoding_work_mem query")
	}

	var workMem *string
	err = rows.Scan(&workMem)
	if err != nil {
		panic(fmt.Errorf("unable to read value for logical_decoding_work_mem: %w", err))
	}

	if workMem == nil {
		// PG version 12 - logical_decoding_work_mem is not available
		return
	}
	const expectedValue = "64kb"
	if strings.ToLower(*workMem) != expectedValue {
		panic("logical_decoding_work_mem config option must be set to 64kb. run sql: 'alter system set logical_decoding_work_mem = '64kB'' and then 'select pg_reload_conf()'")
	}
}

func MustCreateDbConnectionFromConfig(config pgconfig.PgConfig) *pgxpool.Pool {
	all, err := config.Conn.AllOpts()
	if err != nil {
		panic(err)
	}

	conn, err := pgxpool.New(context.Background(), all[0].CreateConnStr())
	if err != nil {
		panic(err)
	}

	return conn
}

func MustCreateDbConnectionFromEnv(ctx context.Context) *pgxpool.Pool {
	pgConnStr := os.Getenv(EnvPgTestConn)
	if pgConnStr == "" {
		panic("PG_TEST_CONN not set")
	}

	conn, err := pgxpool.New(ctx, pgConnStr)
	if err != nil {
		panic(err)
	}

	return conn
}

type TableNameProvider interface {
	TableNames() []string
	CreateScripts() []string
}

func MustCreateTablesSlotAndPubWithProviderAndConfig(ctx context.Context, pgConfig pgconfig.PgConfig, slotConfig pgconfig.PgReplConfig,
	tableProvider TableNameProvider) ([]string, string, string) {
	conn := MustCreateDbConnectionFromConfig(pgConfig)
	providerTables := tableProvider.TableNames()
	sqlScripts := tableProvider.CreateScripts()

	if len(providerTables) != len(sqlScripts) {
		panic("provider and sql scripts don't match")
	}

	for i := 0; i < len(providerTables); i++ {
		table := providerTables[i]
		sqlScript := sqlScripts[i]
		err := createTableFromScript(ctx, conn, table, sqlScript)

		if err != nil {
			panic(err)
		}
	}

	err := createSlotAndPub(ctx, conn, providerTables, slotConfig.Slot, slotConfig.Pub)
	if err != nil {
		panic(err)
	}

	return providerTables, slotConfig.Slot, slotConfig.Pub
}

func MustCreateTableSlotAndPubWithConfig(ctx context.Context, pgConfig pgconfig.PgConfig, slotConfig pgconfig.PgReplConfig) (Table, Slot, Pub) {
	conn := MustCreateDbConnectionFromConfig(pgConfig)
	tableName := fmt.Sprintf("%s_table", slotConfig.Slot)
	slotName := slotConfig.Slot
	pubName := slotConfig.Pub

	err := createTable(ctx, conn, tableName)
	if err != nil {
		panic(err)
	}

	err = createSlotAndPub(ctx, conn, []string{tableName}, slotName, pubName)
	if err != nil {
		panic(err)
	}

	return Table(tableName), Slot(slotName), Pub(pubName)
}

func MustCreateTableSlotAndPub(ctx context.Context, slotPrefix string) (Table, Slot, Pub) {
	b := make([]byte, 4)
	rand.Read(b)
	s := hex.EncodeToString(b)

	conn := MustCreateDbConnectionFromEnv(ctx)
	tableName := fmt.Sprintf("%s_%s", slotPrefix, s)
	slotName := fmt.Sprintf("%s_slot", tableName)
	pubName := fmt.Sprintf("%s_pub", tableName)

	err := createTable(ctx, conn, tableName)
	if err != nil {
		panic(err)
	}

	err = createSlotAndPub(ctx, conn, []string{tableName}, slotName, pubName)
	if err != nil {
		panic(err)
	}

	return Table(tableName), Slot(slotName), Pub(pubName)
}

func Cleanup() {
	conn := MustCreateDbConnectionFromEnv(context.Background())
	jsonTestTables, err := readJsonTestTables(conn)

	if err != nil {
		panic(fmt.Errorf("unable to cleanup test database: %v", err))
	}

	for _, table := range jsonTestTables {
		sName, pName := slotAndPubName(table)
		err := DropSlotAndPub(context.Background(), conn, sName, pName)
		if err != nil {
			panic(fmt.Errorf("unable to drop Slot %s or Pub %s: %w", sName, pName, err))
		}
		err = DropTable(context.Background(), conn, table)
		if err != nil {
			panic(fmt.Errorf("unable to drop Table %s: %w", table, err))
		}
	}
}

func DropTable(ctx context.Context, conn *pgxpool.Pool, tName Table) error {
	dropTable := fmt.Sprintf("DROP TABLE IF EXISTS %s", tName)
	_, err := conn.Exec(ctx, dropTable)
	return err
}

func DropSlotAndPub(ctx context.Context, conn *pgxpool.Pool, sName Slot, pName Pub) error {
	dropPub := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pName)
	_, err := conn.Exec(ctx, dropPub)
	if err != nil {
		return err
	}
	dropSlot := `SELECT pg_drop_replication_slot($1)
				  WHERE EXISTS (SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1)`
	_, err = conn.Exec(ctx, dropSlot, sName)

	return err
}

func MustRunWithTx(ctx context.Context, f func(tx pgx.Tx) error) uint32 {
	conn := MustCreateDbConnectionFromEnv(ctx)
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		panic(err)
	}

	defer tx.Commit(ctx)

	err = f(tx)
	if err != nil {
		panic(err)
	}

	xid := MustReadXid(ctx, tx)

	return xid
}

func MustReadXid(ctx context.Context, tx pgx.Tx) uint32 {
	r, err := tx.Query(ctx, "select txid_current()")
	if err != nil {
		panic(fmt.Errorf("unable to get current xid: %v", err))
	}

	var xid uint32
	for r.Next() {
		err = r.Scan(&xid)
		if err != nil {
			panic(fmt.Errorf("unable to read xid from result: %v", err))
		}
	}

	return xid
}

func createSlotAndPub(ctx context.Context, conn *pgxpool.Pool, tNames []string, sName string, pName string) error {
	slotQuery := "SELECT pg_create_logical_replication_slot($1, 'pgoutput')"
	_, err := conn.Exec(ctx, slotQuery, sName)
	if err != nil {
		return err
	}

	tableNames := strings.Join(tNames, ",")
	pubQuery := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pName, tableNames)
	_, err = conn.Exec(ctx, pubQuery)
	return err
}

func createTable(ctx context.Context, conn *pgxpool.Pool, tName string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
			  (
			    id INTEGER PRIMARY KEY,
			    field_int INTEGER,
			    field_data BYTEA NULL
			  )`, tName)

	return createTableFromScript(ctx, conn, tName, query)
}

func createTableFromScript(ctx context.Context, conn *pgxpool.Pool, tName string, createScript string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tName)
	_, err := conn.Exec(ctx, query)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, createScript)

	// make sure Table's replica identity is set to FULL
	// otherwise "identity" property of the JSON won't be available
	query = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tName)
	_, err = conn.Exec(ctx, query)
	return err
}

func slotAndPubName(tName Table) (Slot, Pub) {
	return Slot(fmt.Sprintf("%s_slot", tName)), Pub(fmt.Sprintf("%s_pub", tName))
}

func readJsonTestTables(conn *pgxpool.Pool) ([]Table, error) {
	tablesQuery := "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'json_test%'"
	res, err := conn.Query(context.Background(), tablesQuery)
	if err != nil {
		return nil, fmt.Errorf("read tables: %w", err)
	}
	defer res.Close()

	var tables []Table
	for res.Next() {
		var tName Table
		err = res.Scan(&tName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, tName)
	}

	return tables, nil
}

func MustCreateReplConnectionFromEnv() *config.ConnectionOptsMany {
	pgConnStr := os.Getenv(EnvPgTestConn)
	if pgConnStr == "" {
		panic("PG_TEST_CONN not set")
	}

	cfg, err := pgx.ParseConfig(pgConnStr)
	if err != nil {
		panic(err)
	}

	return &config.ConnectionOptsMany{
		Host:     cfg.Host,
		Port:     fmt.Sprintf("%d", cfg.Port),
		Database: cfg.Database,
		User:     cfg.User,
		Password: cfg.Password,
	}
}
