package pgconfig

import (
	"errors"

	"github.com/alikonhz/pglogrepl2json/config"
)

var (
	errDbEmpty  = errors.New("PG DB is empty")
	errPwdEmpty = errors.New("PG PWD is empty")
)

type SlotOwner string

const (
	OwnerApp  SlotOwner = "app"
	OwnerUser SlotOwner = "user"
)

type PgNumericMode string

const (
	PgNumericModeFloat  = "float"
	PgNumericModeString = "string"
)

type PgConfig struct {
	Conn           *config.ConnectionOptsMany `json:"conn"`
	Repl           PgReplConfig               `json:"repl"`
	NumericMode    PgNumericMode              `json:"numericMode"`
	StandByTimeout string                     `json:"standByTimeout"`
	ReceiveTimeout string                     `json:"receiveTimeout"`
}

type PgReplConfig struct {
	Slot     string    `json:"slot"`
	Pub      string    `json:"pub"`
	Owner    SlotOwner `json:"owner"`
	Failover *bool     `json:"failover"`
}

func (pgc PgConfig) Validate() error {
	if pgc.Conn == nil {
		return errors.New("connection configuration is empty")
	}
	allOpts, err := pgc.Conn.AllOpts()
	if err != nil {
		return err
	}

	err = validateOpts(allOpts)
	if err != nil {
		return err
	}

	return nil
}

func validateOpts(opts []config.ConnectionOpts) error {
	var err error
	for _, opt := range opts {
		if opt.Database == "" {
			err = errDbEmpty
		}
		if opt.Password == "" && opt.TLS.IsEmpty() {
			err = errors.Join(err, errPwdEmpty)
		}

		tlsErr := opt.TLS.Validate()
		if tlsErr != nil {
			err = errors.Join(err, tlsErr)
		}
	}

	return err
}
