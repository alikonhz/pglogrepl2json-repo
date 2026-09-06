package pgconnector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest/certs"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	userName = "postgres"
	password = "password"
	table    = "testtable"
	slot     = "conn_slot"
	pub      = "conn_pub"
	pgImage  = "docker.io/postgres:16.3"
)

var (
	fiveS = 5 * time.Second
)

var (
	logger = zap.NewNop()
)

func TestConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testOpts := integrationtest.MustCreateReplication(t, ctx, userName, password, pgImage, table, slot, pub)

	many, err := config.JoinOpts([]config.ConnectionOpts{testOpts.PrimOpts, testOpts.ReplicaOpts})
	if err != nil {
		t.Fatal(err)
	}

	connector, err := New(many, logger)
	if err != nil {
		t.Fatal(err)
	}

	err = connector.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, connector.nodes[0].IsPrimary())
	assert.Len(t, connector.nodes, 2)
}

func TestReplicaDownNoError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testOpts := integrationtest.MustCreateReplication(t, ctx, userName, password, pgImage, table, slot, pub)

	many, err := config.JoinOpts([]config.ConnectionOpts{testOpts.PrimOpts, testOpts.ReplicaOpts})
	if err != nil {
		t.Fatal(err)
	}

	connector, err := New(many, logger)
	if err != nil {
		t.Fatal(err)
	}

	err = testOpts.Replica.Stop(ctx, &fiveS)
	if err != nil {
		t.Fatal(err)
	}

	err = connector.Connect(ctx)
	require.NoError(t, err)
	assert.True(t, connector.nodes[0].IsPrimary())
	assert.Len(t, connector.nodes, 2)
	assert.False(t, connector.nodes[1].HasConn())
}

func TestReplicaPromoted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testOpts := integrationtest.MustCreateReplication(t, ctx, userName, password, pgImage, table, slot, pub)
	many, err := config.JoinOpts([]config.ConnectionOpts{testOpts.PrimOpts, testOpts.ReplicaOpts})

	if err != nil {
		t.Fatal(err)
	}

	connector, err := New(many, logger)
	if err != nil {
		t.Fatal(err)
	}

	err = connector.Connect(ctx)
	require.NoError(t, err)

	err = testOpts.Primary.Stop(ctx, &fiveS)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = testOpts.Replica.Exec(ctx, []string{"pg_ctl", "promote"})
	require.NoError(t, err)

	// reconnect
	err = connector.Connect(ctx)
	require.NoError(t, err)
	assert.False(t, connector.nodes[0].IsPrimary())
	assert.True(t, connector.nodes[1].IsPrimary())
}

func TestSpecialPassword(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	specialPassword := "p@ss:word/with?weird#chars+space test"
	// We need to use a simpler username because it might also be used in ENV vars or CMDs that don't like spaces
	// but the issue description specifically mentions password.

	testOpts := integrationtest.MustCreateReplication(t, ctx, userName, specialPassword, pgImage, table, slot, pub)

	many, err := config.JoinOpts([]config.ConnectionOpts{testOpts.PrimOpts, testOpts.ReplicaOpts})
	require.NoError(t, err)

	connector, err := New(many, logger)
	require.NoError(t, err)

	err = connector.Connect(ctx)
	// This is expected to fail because CreateConnStr doesn't escape the password
	require.NoError(t, err, "Connection should succeed even with special characters in password")
}

func TestTLSConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Use absolute path or relative to project root
	// The test runs in pgschema/pgconnector, so we need to go up two levels to reach project root
	tlsDir, err := filepath.Abs("../../internal/integrationtest/testdata/tls")
	require.NoError(t, err)

	err = certs.GenerateCerts(tlsDir)
	require.NoError(t, err)

	// Change working directory to project root so StartPrimaryWithTLS can find the files
	// if it uses relative paths, but we are using absolute paths now.
	// Wait, StartPrimaryWithTLS is in internal/integrationtest, so filepath.Abs("...") there
	// will be relative to wherever the test is running.
	oldWd, err := os.Getwd()
	require.NoError(t, err)
	err = os.Chdir("../..")
	require.NoError(t, err)
	defer os.Chdir(oldWd)

	tlsOpts := config.TLSOptions{
		Cert:     filepath.Join(tlsDir, "client.crt"),
		Key:      filepath.Join(tlsDir, "client.key"),
		RootCert: filepath.Join(tlsDir, "ca.crt"),
	}

	_, _, connOpts := integrationtest.StartPrimaryWithTLS(ctx, userName, password, pgImage, tlsOpts)

	many, err := config.JoinOpts([]config.ConnectionOpts{connOpts})
	require.NoError(t, err)

	connector, err := New(many, logger)
	require.NoError(t, err)

	err = connector.Connect(ctx)
	require.NoError(t, err)
	assert.Len(t, connector.nodes, 1)

	res, err := ExecPrimary(ctx, connector, func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		var ssl string
		if err := conn.QueryRow(ctx, "SHOW ssl").Scan(&ssl); err != nil {
			return false, err
		}

		return ssl == "on", err
	})

	assert.True(t, res)
}
