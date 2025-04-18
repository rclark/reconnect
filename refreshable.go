package reconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	sqltrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/database/sql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type RefreshableDB struct {
	*gorm.DB
}

type BeforeConnect func(ctx context.Context, config *pgx.ConnConfig) error

type options struct {
	ddServiceName string
}

type Option func(*options)

func WithDataDogTracing(serviceName string) Option {
	return func(o *options) {
		o.ddServiceName = serviceName
	}
}

func NewRefreshableDB(dsn string, bc BeforeConnect, opts ...Option) (*RefreshableDB, error) {
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	stdlibOptions := []stdlib.OptionOpenDB{
		// Refresh function updates the pgx.ConnConfig before a new connection is
		// established.
		stdlib.OptionBeforeConnect(bc),

		// For this test, we set up every connection to be discarded after a single
		// use. You would not want to do this in real life.
		stdlib.OptionResetSession(func(ctx context.Context, c *pgx.Conn) error {
			return driver.ErrBadConn
		}),
	}

	var db *sql.DB
	if o.ddServiceName != "" {
		connector := stdlib.GetConnector(*config, stdlibOptions...)
		db = sqltrace.OpenDB(connector, sqltrace.WithServiceName(o.ddServiceName))
	} else {
		db = stdlib.OpenDB(*config, stdlibOptions...)
	}

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return &RefreshableDB{gormDB}, nil
}
