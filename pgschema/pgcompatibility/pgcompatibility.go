package pgcompatibility

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
)

var (
	ErrTrackCommitTimestampDisabled = errors.New("track commit timestamp is disabled")
)

func CheckTrackCommitTimestamp(ctx context.Context, conn *pgx.Conn) error {
	row := conn.QueryRow(ctx, "SHOW track_commit_timestamp")
	var res string
	err := row.Scan(&res)
	if err != nil {
		return err
	}

	if res != "on" {
		return ErrTrackCommitTimestampDisabled
	}

	return nil
}
