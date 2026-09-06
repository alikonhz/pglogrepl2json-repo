package testpgreplicator

import (
	"context"
	"github.com/alikonhz/pglogrepl2json/circuitbreaker"
	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/internal/integrationtest"
	"github.com/alikonhz/pglogrepl2json/jsonparsing/decode"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/alikonhz/pglogrepl2json/replicator"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/zap"
	"testing"
)

type PGOptions struct {
	Image    string
	User     string
	Password string

	CreateTableSQL string

	TableName string
	SlotName  string
	PubName   string

	NumMode decode.NumericEncoding
}

type TestPGReplicatorOptions struct {
	Postgres    PGOptions
	RetryConfig circuitbreaker.RetryConfig
	Logger      *zap.Logger
}

type CreateResult struct {
	Replicator *replicator.PGReplicator
	Pool       *pgxpool.Pool
	Container  testcontainers.Container
	Connector  *pgconnector.PGConnector
	Options    TestPGReplicatorOptions
}

func MustPrepareForPGReplicator(t *testing.T, opts TestPGReplicatorOptions) *CreateResult {
	pgPrimary, _, primOpts := integrationtest.StartPrimary(t.Context(), opts.Postgres.User, opts.Postgres.Password, opts.Postgres.Image)

	t.Cleanup(func() {
		pgPrimary.Terminate(context.Background())
	})

	primPool, err := pgxpool.New(t.Context(), integrationtest.CreatePGConnStr(primOpts))
	if err != nil {
		panic("failed to connect to primary pool: " + err.Error())
	}

	t.Cleanup(func() {
		primPool.Close()
	})

	err = integrationtest.PreparePrimaryForLogicalReplication(primPool, opts.Postgres.CreateTableSQL, opts.Postgres.TableName, opts.Postgres.SlotName, opts.Postgres.PubName)

	require.NoError(t, err)

	optsMany, err := config.JoinOpts([]config.ConnectionOpts{primOpts})

	require.NoError(t, err)

	pgConnector, err := pgconnector.New(optsMany, opts.Logger)

	require.NoError(t, err)

	err = pgConnector.Connect(t.Context())

	require.NoError(t, err)

	t.Cleanup(func() {
		pgConnector.Close(t.Context())
	})

	return &CreateResult{
		Container: pgPrimary,
		Pool:      primPool,
		Connector: pgConnector,
		Options:   opts,
	}
}
