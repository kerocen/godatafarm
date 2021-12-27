package farm

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/rs/zerolog"
	"godatafarm/rdbms"
)

type Farm struct {
	log      zerolog.Logger
	postgres *rdbms.Postgres
}

// TODO create othe farms that we can modify the log io.Writer target, the timesamps, and so on (see zerolog.NewConsoleWriter)
// TODO setup a simple github action with linter only
// default farm, logs to console
func NewFarm() *Farm {
	out := zerolog.NewConsoleWriter()
	log := zerolog.New(out).With().Str("component", "go-data-farm").Logger()
	f := &Farm{
		log: log,
	}
	return f
}

func PostgresConnectionBasic(username, password, host string, port int, db string) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", username, password, host, port, db)
}

func (f *Farm) PostgresFromConnectionString(
	connectionString string,
	tables []rdbms.Table,
) error {
	pg, err := rdbms.NewPostgresFromConnectionString(connectionString, tables, f.log)
	if err != nil {
		return fmt.Errorf("failed to attach postgres: %w", err)
	}
	f.postgres = pg
	return nil
}

func (f *Farm) Postgres(
	db *sql.DB,
	tables []rdbms.Table,
) error {
	pg, err := rdbms.NewPostgres(db, tables, f.log)
	if err != nil {
		return fmt.Errorf("failed to attach postgres: %w", err)
	}
	f.postgres = pg
	return nil
}

func (f *Farm) SeedPostgres(ctx context.Context) error {
	err := f.postgres.InsertBulk(ctx)
	if err != nil {
		return fmt.Errorf("failed to bulk insert postgres: %w", err)
	}
	return nil
}
