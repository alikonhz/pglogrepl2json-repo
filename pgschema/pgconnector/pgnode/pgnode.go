package pgnode

import (
	"context"
	"fmt"
	"sync"

	"github.com/alikonhz/pglogrepl2json/pgschema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNodeNotConnected = fmt.Errorf("node is not connected")
)

type PGNode struct {
	isPrimary  bool
	replConfig *pgconn.Config
	connConfig *pgx.ConnConfig
	hostPort   string

	mu   *sync.Mutex
	conn *pgx.Conn

	version pgschema.PGVersion
}

func New(replConfig *pgconn.Config, connConfig *pgx.ConnConfig) *PGNode {
	return &PGNode{
		mu:         &sync.Mutex{},
		conn:       nil,
		isPrimary:  false,
		replConfig: replConfig,
		connConfig: connConfig,
		hostPort:   fmt.Sprintf("%s:%d", connConfig.Host, connConfig.Port),
		version:    pgschema.Unknown,
	}
}

func (n *PGNode) PGVersion() pgschema.PGVersion {
	return n.version
}

func (n *PGNode) IsPrimary() bool {
	return n.isPrimary
}

func (n *PGNode) HasConn() bool {
	n.mu.Lock()
	res := n.conn != nil
	n.mu.Unlock()

	return res
}

func (n *PGNode) Close(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn != nil {
		conn := n.conn
		n.conn = nil

		return conn.Close(ctx)
	}

	return nil
}

type ExecFunc[T any] func(ctx context.Context, conn *pgx.Conn) (T, error)

func WithConnection[T any](ctx context.Context, node *PGNode, f ExecFunc[T]) (T, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.conn == nil {
		var t T

		return t, fmt.Errorf("%w: %s", ErrNodeNotConnected, node.HostPort())
	}

	res, err := f(ctx, node.conn)

	return res, err
}

func (n *PGNode) ConnectReplication(ctx context.Context) (*pgconn.PgConn, error) {
	c, err := pgconn.ConnectConfig(ctx, n.replConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to connect via replication connection: %w", err)
	}

	return c, nil
}

func (n *PGNode) Connect(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.isPrimary = false

	conn, err := n.getConnOrConnect(ctx)
	if err != nil {
		return err
	}

	err = conn.Ping(ctx)
	if err != nil {
		// retry one time
		if n.conn != nil {
			n.conn.Close(ctx)
			n.conn = nil
		}

		conn, err = n.connectNew(ctx)
		if err != nil {
			return err
		}

		err = conn.Ping(ctx)
		if err != nil {
			return fmt.Errorf("failed to ping node %s:%d: %w", n.connConfig.Host, n.connConfig.Port, err)
		}
	}

	row := conn.QueryRow(ctx, "select pg_is_in_recovery()")

	var inRecovery bool

	err = row.Scan(&inRecovery)

	if err != nil {
		return fmt.Errorf("failed to query status of node %s:%d: %w", n.connConfig.Host, n.connConfig.Port, err)
	}

	n.isPrimary = !inRecovery

	ver, err := pgschema.ReadPGVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to read version of node %s:%d: %w", n.connConfig.Host, n.connConfig.Port, err)
	}

	n.version = ver

	return nil
}

func (n *PGNode) getConnOrConnect(ctx context.Context) (*pgx.Conn, error) {
	if n.conn != nil {
		return n.conn, nil
	}

	conn, err := n.connectNew(ctx)

	return conn, err
}

func (n *PGNode) connectNew(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, n.connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s:%d: %w", n.connConfig.Host, n.connConfig.Port, err)
	}

	n.conn = conn

	return conn, nil
}

func (n *PGNode) ConnConfig() (*pgx.ConnConfig, error) {
	return n.connConfig.Copy(), nil
}

func (n *PGNode) HostPort() string {
	return n.hostPort
}
