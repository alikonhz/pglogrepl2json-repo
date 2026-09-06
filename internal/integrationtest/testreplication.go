package integrationtest

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	pgCont "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ReplicationTest struct {
	Primary  testcontainers.Container
	PrimPool *pgxpool.Pool
	PrimOpts config.ConnectionOpts

	Replica     testcontainers.Container
	ReplicaPool *pgxpool.Pool
	ReplicaOpts config.ConnectionOpts
}

func (rt *ReplicationTest) OptsMany() (*config.ConnectionOptsMany, error) {
	return config.JoinOpts([]config.ConnectionOpts{rt.PrimOpts, rt.ReplicaOpts})
}

func MustCreateReplication(t *testing.T, ctx context.Context, user, pwd, image, table, slot, pub string) *ReplicationTest {
	pgPrimary, primIP, primOpts := StartPrimary(ctx, user, pwd, image)
	pgReplica, _, replicaOpts := startReplica(ctx, primIP, user, pwd, image)

	primPool, err := pgxpool.New(ctx, CreatePGConnStr(primOpts))
	if err != nil {
		panic("failed to connect to primary pool: " + err.Error())
	}

	replPool, err := pgxpool.New(ctx, CreatePGConnStr(replicaOpts))
	if err != nil {
		panic("failed to connect to repl pool: " + err.Error())
	}

	t.Cleanup(func() {
		replPool.Close()
		primPool.Close()

		pgReplica.Terminate(context.Background())
		pgPrimary.Terminate(context.Background())
	})

	err = PreparePrimaryForLogicalReplication(primPool,
		fmt.Sprintf("CREATE TABLE %s (id int not null primary key, data text not null) ", table),
		table,
		slot,
		pub)

	if err != nil {
		panic("failed to prepare primary: " + err.Error())
	}

	// wait for replication
	time.Sleep(2 * time.Second)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	errChan := prepareReplicaForLogicalReplication(replPool, wg, string(slot))

	// CHECKPOINT is needed to make sure logical replication slot will be created on the replica without hanging
	// waiting for the replica to send creation slot query
	wg.Wait()
	// waiting until query makes it to the server
	time.Sleep(500 * time.Millisecond)

	_, err = primPool.Exec(ctx, "checkpoint;")
	if err != nil {
		panic("failed to prepare replica: " + err.Error())
	}
	err = <-errChan
	if err != nil {
		panic("failed to prepare replica: " + err.Error())
	}

	rows, err := replPool.Query(ctx, "select * from "+string(table))
	if err != nil {
		panic("replication setup failed:" + err.Error())
	}
	rows.Close()

	return &ReplicationTest{
		Primary:  pgPrimary,
		PrimPool: primPool,
		PrimOpts: primOpts,

		Replica:     pgReplica,
		ReplicaPool: replPool,
		ReplicaOpts: replicaOpts,
	}
}

func StartPrimary(ctx context.Context, userName, password, pgImage string) (testcontainers.Container, string, config.ConnectionOpts) {
	return StartPrimaryWithTLS(ctx, userName, password, pgImage, config.TLSOptions{})
}

func StartPrimaryWithTLS(ctx context.Context, userName, password, pgImage string, tls config.TLSOptions) (testcontainers.Container, string, config.ConnectionOpts) {
	var primaryCMD testcontainers.CustomizeRequestOption = func(req *testcontainers.GenericContainerRequest) error {
		pgCmd := []string{
			"postgres",
			"-c", "fsync=off",
			"-c", "wal_level=logical",
			"-c", "hot_standby=on",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
			"-c", "hot_standby_feedback=on",
		}

		if !tls.IsEmpty() {
			pgCmd = append(pgCmd,
				"-c", "ssl=on",
				"-c", "ssl_cert_file=/tmp/certs/server.crt",
				"-c", "ssl_key_file=/tmp/certs/server.key",
				"-c", "ssl_ca_file=/tmp/certs/ca.crt",
			)
			// Use a wrapper to fix permissions before starting postgres
			req.User = "root"
			req.Cmd = []string{
				"bash", "-c",
				"chown -R postgres:postgres /tmp/certs && chmod 600 /tmp/certs/server.key && gosu postgres docker-entrypoint.sh " + strings.Join(pgCmd, " "),
			}
		} else {
			req.Cmd = pgCmd
		}

		return nil
	}

	var primaryENV testcontainers.CustomizeRequestOption = func(req *testcontainers.GenericContainerRequest) error {
		req.Env["POSTGRES_HOST_AUTH_METHOD"] = "scram-sha-256\nhost replication all 0.0.0.0/0 md5"
		req.Env["POSTGRES_INITDB_ARGS"] = "--auth-host=scram-sha-256"
		req.Env["POSTGRES_DB"] = "postgres" // default dbName remains postgres
		req.Env["POSTGRES_USER"] = userName
		req.Env["POSTGRES_PASSWORD"] = password

		return nil
	}

	var args = []testcontainers.ContainerCustomizer{
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5 * time.Second)),
		primaryENV,
		primaryCMD,
	}

	if !tls.IsEmpty() {
		serverCert, _ := filepath.Abs("internal/integrationtest/testdata/tls/server.crt")
		serverKey, _ := filepath.Abs("internal/integrationtest/testdata/tls/server.key")
		caCert, _ := filepath.Abs("internal/integrationtest/testdata/tls/ca.crt")

		args = append(args,
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      serverCert,
				ContainerFilePath: "/tmp/certs/server.crt",
				FileMode:          0644,
			}),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      serverKey,
				ContainerFilePath: "/tmp/certs/server.key",
				FileMode:          0640,
			}),
			testcontainers.WithFiles(testcontainers.ContainerFile{
				HostFilePath:      caCert,
				ContainerFilePath: "/tmp/certs/ca.crt",
				FileMode:          0644,
			}),
		)
	}

	if !tls.IsEmpty() {
		args = append(args,
			testcontainers.WithConfigModifier(func(config *container.Config) {
				config.User = "0:0"
			}),
		)
	}

	pgPrim, err := pgCont.Run(ctx, pgImage, args...)

	if err != nil {
		panic("failed to start container: " + err.Error())
	}

	primEndpoint, err := pgPrim.Endpoint(ctx, "")
	if err != nil {
		panic("failed to get endpoint: " + err.Error())
	}

	hostPort := strings.Split(primEndpoint, ":")

	primIP, err := pgPrim.ContainerIP(ctx)
	if err != nil {
		panic("failed to get container IP: " + err.Error())
	}

	primHost, primPort := hostPort[0], hostPort[1]

	connOpts := config.ConnectionOpts{
		Host:     primHost,
		Port:     primPort,
		Database: "postgres",
		User:     userName,
		Password: password,
		TLS:      tls,
	}

	primConfig := connOpts.CreateConnStr()

	conn, err := pgx.Connect(ctx, primConfig)
	if err != nil {
		panic("failed to connect to primary container: " + err.Error())
	}

	defer conn.Close(ctx)

	_, err = conn.Exec(ctx,
		`CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_password';
SELECT pg_create_physical_replication_slot('replication_slot');`)

	if err != nil {
		panic("failed to setup replication on primary: " + err.Error())
	}

	return pgPrim, primIP, connOpts
}

func startReplica(ctx context.Context, primIP, userName, password, pgImage string) (testcontainers.Container, string, config.ConnectionOpts) {
	// pg_basebackup will be executed inside Docker network
	// hence we need internal IP address (see --host=%s)
	replicaCMD := []string{
		"bash", "-c",
		fmt.Sprintf(
			`
if [ ! -d /var/lib/postgresql/data/base ] ; then
        until pg_basebackup --pgdata=/var/lib/postgresql/data -R --slot=replication_slot --host=%s --port=5432
        do
           echo 'Waiting for primary to connect...'
           sleep 1s
        done
        chmod 0700 /var/lib/postgresql/data
      else
        echo 'Dir exists';
      fi;
      postgres -c wal_level=logical -c max_replication_slots=10 -c hot_standby_feedback=on
`, primIP),
	}

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: pgImage,
			Env: map[string]string{
				"PGUSER":     "replicator",
				"PGPASSWORD": "replicator_password",
			},
			WaitingFor: wait.ForLog("started streaming WAL from primary").
				WithOccurrence(1).
				WithStartupTimeout(5 * time.Second).
				WithPollInterval(1 * time.Second),
			Cmd:  replicaCMD,
			User: userName,
		},
		Started: true,
	}

	pgReplica, err := testcontainers.GenericContainer(ctx, req)

	if err != nil {
		panic("failed to start replica container: " + err.Error())
	}

	replicaEndpoint, err := pgReplica.Endpoint(ctx, "")
	if err != nil {
		panic("failed to get replica's endpoint: " + err.Error())
	}

	hostPort := strings.Split(replicaEndpoint, ":")
	replicaIP, err := pgReplica.ContainerIP(ctx)
	if err != nil {
		panic("failed to get replica IP: " + err.Error())
	}

	replHost, replPort := hostPort[0], hostPort[1]
	return pgReplica, replicaIP, config.ConnectionOpts{
		Host:     replHost,
		Port:     replPort,
		Database: "postgres",
		User:     userName,
		Password: password,
		TLS:      config.TLSOptions{},
	}
}

func prepareReplicaForLogicalReplication(pool *pgxpool.Pool, wg *sync.WaitGroup, slotName string) <-chan error {
	ch := make(chan error)

	go func() {
		// waiting for replication to catch up
		time.Sleep(1 * time.Second)
		wg.Done()
		err := createSlot(pool, slotName)
		if err != nil {
			ch <- fmt.Errorf("failed to create slot %s on replica: %w", slotName, err)
		}

		ch <- nil

		close(ch)
	}()
	return ch
}

func PreparePrimaryForLogicalReplication(pool *pgxpool.Pool, createSQL, tableName, slotName, pubName string) error {
	_, err := pool.Exec(context.Background(), createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	err = createSlot(pool, slotName)
	if err != nil {
		return fmt.Errorf("failed to create slot %s on primary: %w", slotName, err)
	}

	err = createPublication(context.Background(), pool, pubName, tableName)
	if err != nil {
		return fmt.Errorf("failed to create publication %s for table %s: %w", pubName, tableName, err)
	}

	return nil
}

func createPublication(ctx context.Context, pool *pgxpool.Pool, pubName, tableName string) error {
	_, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, tableName))

	return err
}

func createSlot(pool *pgxpool.Pool, slotName string) error {
	_, err := pool.Exec(context.Background(), fmt.Sprintf("select pg_create_logical_replication_slot('%s', 'pgoutput')", slotName))
	return err
}

func CreatePGConnStr(opts config.ConnectionOpts) string {
	return opts.CreateConnStr()
}
