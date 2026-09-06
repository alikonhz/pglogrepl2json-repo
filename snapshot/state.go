package snapshot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type SnapshotStatus string

const (
	StatusPending    SnapshotStatus = "pending"
	StatusInProgress SnapshotStatus = "in_progress"
	StatusCompleted  SnapshotStatus = "completed"
	StatusFailed     SnapshotStatus = "failed"
)

type TableState struct {
	AppName       string  `json:"app_name"`
	TableName     string  `json:"table_name"`
	SnapshotLSN   string  `json:"snapshot_lsn"`
	SnapshotType  string  `json:"snapshot_type"`
	SnapshotQuery *string `json:"snapshot_query"`

	TotalRows     *int64         `json:"total_rows"`
	ProcessedRows int64          `json:"processed_rows"`
	Status        SnapshotStatus `json:"status"`
	StartedAt     *time.Time     `json:"started_at"`
	UpdatedAt     *time.Time     `json:"updated_at"`
	SnapshotStart *time.Time     `json:"snapshot_start"`
	SnapshotEnd   *time.Time     `json:"snapshot_end"`
	ErrorMessage  *string        `json:"error_message"`

	whereClause string
}

func (t *TableState) TotalRowsValue() int64 {
	if t.TotalRows == nil {
		return 0
	}

	return *t.TotalRows
}

const CreateStateTableSQL = `
CREATE SCHEMA IF NOT EXISTS pgwalk;
CREATE TABLE IF NOT EXISTS pgwalk.snapshot_state (
    app_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    snapshot_lsn TEXT NOT NULL,
    snapshot_type TEXT NOT NULL,  -- 'full' | 'query'
    snapshot_query TEXT,
    resume_key_column TEXT,       
    resume_key_value TEXT,        
    total_rows BIGINT,            -- Total rows to process (if known)
    processed_rows BIGINT,        -- Rows successfully processed
    status TEXT NOT NULL,         -- 'pending' | 'in_progress' | 'completed' | 'failed'
    started_at TIMESTAMP,
    updated_at TIMESTAMP,
    snapshot_start TIMESTAMP,
    snapshot_end TIMESTAMP,
    error_message TEXT,
    CONSTRAINT pk_pgwal_snapshot_state PRIMARY KEY (app_name, table_name)
);
`

type StateManager struct {
	connector *pgconnector.PGConnector
	appName   string

	mu   sync.Mutex
	conn *pgx.Conn

	logger *zap.Logger
}

func NewStateManager(connector *pgconnector.PGConnector, appName string, logger *zap.Logger) *StateManager {
	return &StateManager{
		connector: connector,
		appName:   appName,
		logger:    logger,
	}
}

func (s *StateManager) withConnection(ctx context.Context, f func(ctx context.Context, conn *pgx.Conn) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.connection(ctx)
	if err != nil {
		return err
	}

	return f(ctx, conn)
}

func (s *StateManager) connection(ctx context.Context) (*pgx.Conn, error) {
	if s.conn != nil && !s.conn.IsClosed() {
		return s.conn, nil
	}

	conn, err := s.connector.AcquirePrimary(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire primary connection: %w", err)
	}

	s.conn = conn
	return conn, nil
}

func (s *StateManager) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return nil
	}

	conn := s.conn
	s.conn = nil

	return conn.Close(ctx)
}

func (s *StateManager) CreateSnapshotStateTableIfNotExists(ctx context.Context) error {
	return s.withConnection(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, CreateStateTableSQL)
		return err
	})
}

func (s *StateManager) LoadTablesState(ctx context.Context) (map[string]*TableState, error) {
	var states map[string]*TableState

	err := s.withConnection(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "SELECT table_name, snapshot_lsn, snapshot_type, snapshot_query, total_rows, processed_rows, status, started_at, updated_at, snapshot_start, snapshot_end, error_message FROM pgwalk.snapshot_state WHERE app_name = $1", s.appName)
		if err != nil {
			return err
		}
		defer rows.Close()

		states = make(map[string]*TableState)
		for rows.Next() {
			var state TableState
			state.AppName = s.appName
			err := rows.Scan(&state.TableName, &state.SnapshotLSN, &state.SnapshotType, &state.SnapshotQuery, &state.TotalRows, &state.ProcessedRows, &state.Status, &state.StartedAt, &state.UpdatedAt, &state.SnapshotStart, &state.SnapshotEnd, &state.ErrorMessage)
			if err != nil {
				return err
			}
			states[state.TableName] = &state
		}
		return rows.Err()
	})

	return states, err
}

func (s *StateManager) UpsertTableState(ctx context.Context, state *TableState) error {
	return s.withConnection(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `
			INSERT INTO pgwalk.snapshot_state (app_name, table_name, snapshot_lsn, snapshot_type, snapshot_query, total_rows, processed_rows, status, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
			ON CONFLICT ON CONSTRAINT pk_pgwal_snapshot_state DO UPDATE SET
				snapshot_lsn = $3,
				snapshot_type = $4,
				snapshot_query = $5,
				total_rows = $6,
				processed_rows = $7,
				status = $8,
				snapshot_start = NULL,
				snapshot_end = NULL,
				updated_at = NOW()
		`, s.appName, state.TableName, state.SnapshotLSN, state.SnapshotType, state.SnapshotQuery, state.TotalRows, state.ProcessedRows, state.Status)
		return err
	})
}

func (s *StateManager) UpdateProgress(ctx context.Context, tableName string, processedRows int64, status SnapshotStatus, errorMessage *string) error {
	return s.withConnection(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		query, args, ok := s.progressUpdateStatement(tableName, processedRows, status, errorMessage)
		if !ok {
			return nil
		}

		_, err := conn.Exec(ctx, query, args...)
		return err
	})
}

func (s *StateManager) progressUpdateStatement(tableName string, processedRows int64, status SnapshotStatus, errorMessage *string) (string, []any, bool) {
	if status == StatusInProgress && processedRows > 0 {
		return `
				UPDATE pgwalk.snapshot_state SET
					processed_rows = $3,
					status = $4,
					updated_at = NOW()
				WHERE app_name = $1 AND table_name = $2
			`, []any{s.appName, tableName, processedRows, status}, true
	}

	if status == StatusCompleted && processedRows > 0 {
		return `
				UPDATE pgwalk.snapshot_state SET
					processed_rows = $3,
					status = $4,
					error_message = $5,
					snapshot_end = NOW(),
					updated_at = NOW()
				WHERE app_name = $1 AND table_name = $2
			`, []any{s.appName, tableName, processedRows, status, errorMessage}, true
	}

	if status == StatusCompleted {
		return `
					UPDATE pgwalk.snapshot_state SET
						status = $3,
						error_message = $4,
						snapshot_end = NOW(),
						updated_at = NOW()
					WHERE app_name = $1 AND table_name = $2
				`, []any{s.appName, tableName, status, errorMessage}, true
	}

	if status == StatusFailed {
		return `
					UPDATE pgwalk.snapshot_state SET 
						status = $3,
						error_message = $4,
						updated_at = NOW()
					WHERE app_name = $1 AND table_name = $2
				`, []any{s.appName, tableName, status, errorMessage}, true
	}

	return "", nil, false
}

func (s *StateManager) SetInProgress(ctx context.Context, tableName string, totalRows int64) error {
	return s.withConnection(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `
			UPDATE pgwalk.snapshot_state SET 
				status = $3,
				total_rows = $4,
				started_at = NOW(),
				snapshot_start = NOW(),
				snapshot_end = NULL,
				updated_at = NOW()
			WHERE app_name = $1 AND table_name = $2
		`, s.appName, tableName, StatusInProgress, totalRows)
		return err
	})
}
