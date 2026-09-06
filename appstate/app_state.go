package appstate

import (
	"context"
	"github.com/alikonhz/pglogrepl2json/pgschema/pgconnector"
	"github.com/jackc/pgx/v5"
)

const createSQL = `
CREATE SCHEMA IF NOT EXISTS pgwalk;
CREATE TABLE IF NOT EXISTS pgwalk.app_state
(
 app_name TEXT NOT NULL PRIMARY KEY,
 state TEXT NOT NULL,
 last_xid TEXT NOT NULL
);
`

type AppState struct {
	AppName string
	State string
	LastXID string
}

func Create(ctx context.Context, connector *pgconnector.PGConnector) error {
	_, err := pgconnector.ExecPrimary[struct{}](ctx, connector, func(ctx context.Context, conn *pgx.Conn) (struct{}, error) {
		_, err := conn.Exec(ctx, createSQL)

		return struct{}{}, err
	})

	return err
}

func Save(ctx context.Context, connector *pgconnector.PGConnector, state *AppState) error {
	_, err := pgconnector.ExecPrimary(ctx, connector, func(ctx context.Context, conn *pgx.Conn) (struct{}, error) {
		_, err := conn.Exec(ctx, `
INSERT INTO pgwalk.app_state (app_name, state, last_xid)
VALUES ($1, $2, $3)
ON CONFLICT (app_name) DO UPDATE SET state = $2, last_xid = $3
`, state.AppName, state.State, state.LastXID)

		return struct{}{}, err
	})

	return err
}

func Load(ctx context.Context, connector *pgconnector.PGConnector, appName string) (*AppState, error) {
	var state, lastXID string

	_, err := pgconnector.ExecPrimary[struct{}](ctx, connector, func(ctx context.Context, conn *pgx.Conn) (struct{}, error) {
		err := conn.QueryRow(ctx, `
SELECT state, last_xid
FROM pgwalk.app_state
WHERE app_name = $1
`, appName).Scan(&state, &lastXID)

		return struct{}{}, err
	})

	if err != nil {
		return nil, err
	}

	return &AppState{
		AppName: appName,
		State:   state,
		LastXID: lastXID,
	}, nil
}