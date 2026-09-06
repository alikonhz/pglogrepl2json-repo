package pgconnector

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector/pgnode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

var (
	ErrPrimNodeNotAvailable      = errors.New("primary node not available")
	ErrUnableToConnectToPrimNode = errors.New("unable to connect to primary node")
)

type PGConnector struct {
	logger *zap.Logger
	nodes  []*pgnode.PGNode
}

func NewWithApp(opts *config.ConnectionOptsMany, appName string, logger *zap.Logger) (*PGConnector, error) {
	allOpts, err := opts.AllOpts()
	if err != nil {
		return nil, fmt.Errorf("connector: failed to parse options: %w", err)
	}

	nodes := make([]*pgnode.PGNode, len(allOpts))

	for i := 0; i < len(allOpts); i++ {
		node, err := createNode(allOpts[i].CreateConnStr(), appName)
		if err != nil {
			return nil, err
		}

		nodes[i] = node
	}

	return &PGConnector{
		logger: logger,
		nodes:  nodes,
	}, nil
}

func New(opts *config.ConnectionOptsMany, logger *zap.Logger) (*PGConnector, error) {
	return NewWithApp(opts, "", logger)
}

func createNode(connStr string, appName string) (*pgnode.PGNode, error) {
	var (
		err     error
		replCfg *pgconn.Config
		cfg     *pgx.ConnConfig
	)

	replCfg, err = pgconn.ParseConfig(connStr)

	if err != nil {
		return nil, fmt.Errorf("connector: failed to parse connection string: %w", err)
	}

	if repl, ok := replCfg.RuntimeParams["replication"]; !ok || repl != "database" {
		replCfg.RuntimeParams["replication"] = "database"
	}

	if appName != "" {
		replCfg.RuntimeParams["application_name"] = appName + "_repl"
	}

	cfg, err = pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("connector: failed to parse connection string: %w", err)
	}

	if appName != "" {
		cfg.RuntimeParams["application_name"] = appName
	}

	return pgnode.New(replCfg, cfg), nil
}

func (c *PGConnector) ConnectPrimaryNode(ctx context.Context) (string, error) {
	err := c.Connect(ctx)

	if err != nil {
		return "", fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	node, err := c.PrimaryNode()

	if err != nil {
		return "", fmt.Errorf("failed to connect to primary node: %w", err)
	}

	c.logger.Info("connected to primary node", zap.String("host_port", node))

	return node, nil
}

func (c *PGConnector) Close(ctx context.Context) error {
	for _, node := range c.nodes {
		node.Close(ctx)
	}

	return nil
}

func (c *PGConnector) Connect(ctx context.Context) error {

	if ctx.Err() != nil {
		return ctx.Err()
	}

	wg := sync.WaitGroup{}
	wg.Add(len(c.nodes))

	var results = make([]error, len(c.nodes))

	for i := 0; i < len(c.nodes); i++ {
		curNode := c.nodes[i]
		nodeIndex := i

		go func() {
			defer wg.Done()

			err := curNode.Connect(ctx)
			if err != nil {
				results[nodeIndex] = err
			}
		}()
	}

	wg.Wait()

	for _, err := range results {
		if err != nil {
			c.logger.Warn("failed to connect to PG node", zap.Error(err))
		}
	}

	for _, node := range c.nodes {
		if node.IsPrimary() {
			return nil
		}
	}

	// we didn't find a primary node - return an error
	return ErrUnableToConnectToPrimNode
}

func (c *PGConnector) PrimaryNode() (string, error) {
	for _, node := range c.nodes {
		if node.IsPrimary() && node.HasConn() {
			return node.HostPort(), nil
		}
	}

	return "", ErrPrimNodeNotAvailable
}

func (c *PGConnector) PrimaryVersion() (pgschema.PGVersion, error) {
	for _, node := range c.nodes {
		if node.IsPrimary() {
			return node.PGVersion(), nil
		}
	}

	return pgschema.Unknown, ErrPrimNodeNotAvailable
}

func (c *PGConnector) AcquirePrimary(ctx context.Context) (*pgx.Conn, error) {
	for _, node := range c.nodes {
		if node.IsPrimary() {
			allOpts, err := node.ConnConfig()
			if err != nil {
				return nil, err
			}
			return pgx.ConnectConfig(ctx, allOpts)
		}
	}

	return nil, ErrPrimNodeNotAvailable
}

func (c *PGConnector) GetPrimaryReplConn(ctx context.Context) (*pgconn.PgConn, error) {
	for _, node := range c.nodes {
		if node.IsPrimary() {
			return node.ConnectReplication(ctx)
		}
	}

	return nil, ErrPrimNodeNotAvailable
}

func (c *PGConnector) ReplicasCount() int {
	count := 0

	for _, node := range c.nodes {
		if !node.IsPrimary() {
			count++
		}
	}

	return count
}

func (c *PGConnector) NodeVersions() map[string]pgschema.PGVersion {
	versions := make(map[string]pgschema.PGVersion, len(c.nodes))
	for _, node := range c.nodes {
		versions[node.HostPort()] = node.PGVersion()
	}

	return versions
}

func ExecPrimary[T any](ctx context.Context, connector *PGConnector, f pgnode.ExecFunc[T]) (T, error) {
	var t T

	for _, node := range connector.nodes {
		if node.IsPrimary() {
			res, err := pgnode.WithConnection(ctx, node, f)
			if err != nil {
				return t, err
			}

			return res, nil
		}
	}

	return t, ErrPrimNodeNotAvailable
}

func ExecReplica[T any](ctx context.Context, connector *PGConnector, f pgnode.ExecFunc[T]) (T, error) {
	var (
		allErr error
		t      T
	)

	for _, node := range connector.nodes {
		if !node.IsPrimary() {
			if !node.HasConn() {
				allErr = errors.Join(allErr, fmt.Errorf("%w: %s", pgnode.ErrNodeNotConnected, node.HostPort()))

				continue
			}

			_, err := pgnode.WithConnection(ctx, node, f)
			if err != nil {
				allErr = errors.Join(allErr, fmt.Errorf("failed to execute query on replica node %s: %w", node.HostPort(), err))
			}
		}
	}

	return t, allErr
}
