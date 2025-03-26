package reconnect

import (
	"context"
	"database/sql/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type RefreshableDB struct {
	*gorm.DB
}

type Refresher func(ctx context.Context, config *pgx.ConnConfig) error

func NewRefreshableDB(dsn string, refresh Refresher) (*RefreshableDB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	db := stdlib.OpenDB(*config,
		// Refresh function updates the pgx.ConnConfig before a new connection is established.
		stdlib.OptionBeforeConnect(refresh),

		// For this test, we set up every connection to be discarded after a single use.
		stdlib.OptionResetSession(func(ctx context.Context, c *pgx.Conn) error {
			return driver.ErrBadConn
		}),
	)

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return &RefreshableDB{gormDB}, nil
}
