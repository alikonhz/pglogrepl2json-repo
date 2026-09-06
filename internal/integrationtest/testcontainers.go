package integrationtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alikonhz/pglogrepl2json/config"
	"github.com/testcontainers/testcontainers-go"
	pgCont "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	PGImages = []string{
		"docker.io/postgres:18",
		"docker.io/postgres:17",
		"docker.io/postgres:16",
		"docker.io/postgres:15",
		"docker.io/postgres:14",
	}
)

func CreatePGContainer(ctx context.Context, pgImage string, prefix string, opts ...testcontainers.ContainerCustomizer) (*pgCont.PostgresContainer, error) {

	pgC, connOpts, err := CreatePGContainerOpts(ctx, pgImage, prefix, opts...)
	if err != nil {
		return nil, err
	}

	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_HOST", strings.ToUpper(prefix)), connOpts.Host)
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_PORT", strings.ToUpper(prefix)), connOpts.Port)
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_DATABASE", strings.ToUpper(prefix)), connOpts.Database)
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_USER", strings.ToUpper(prefix)), connOpts.User)
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_PASSWORD", strings.ToUpper(prefix)), connOpts.Password)
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_TLS_CERT", strings.ToUpper(prefix)), "")
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_TLS_KEY", strings.ToUpper(prefix)), "")
	os.Setenv(fmt.Sprintf("%s_POSTGRES_CONN_TLS_ROOTCERT", strings.ToUpper(prefix)), "")
	os.Setenv(fmt.Sprintf("%s_POSTGRES_REPL_SLOT", strings.ToUpper(prefix)), fmt.Sprintf("%s_slot", strings.ToLower(prefix)))
	os.Setenv(fmt.Sprintf("%s_POSTGRES_REPL_PUB", strings.ToUpper(prefix)), fmt.Sprintf("%s_publication", strings.ToLower(prefix)))

	return pgC, nil
}

func CreatePGContainerOpts(ctx context.Context, pgImage string, prefix string, opts ...testcontainers.ContainerCustomizer) (*pgCont.PostgresContainer, *config.ConnectionOptsMany, error) {
	var args []testcontainers.ContainerCustomizer = []testcontainers.ContainerCustomizer{
		testcontainers.WithImage(pgImage),
		pgCont.WithUsername("postgres"),
		pgCont.WithPassword("password"),
		pgCont.WithDatabase(strings.ToLower(prefix)),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5 * time.Second)),
	}
	if len(opts) > 0 {
		args = append(args, opts...)
	}

	pgC, err := pgCont.RunContainer(ctx, args...)

	if err != nil {
		return nil, nil, err
	}

	p, err := pgC.Endpoint(ctx, "")
	hostPort := strings.Split(p, ":")
	pgConnOpts := &config.ConnectionOptsMany{
		Host:     hostPort[0],
		Port:     hostPort[1],
		Database: strings.ToLower(prefix),
		User:     "postgres",
		Password: "password",
	}

	return pgC, pgConnOpts, nil
}
