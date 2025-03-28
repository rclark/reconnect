package reconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
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
		// Refresh function updates the pgx.ConnConfig before a new connection is established.
		stdlib.OptionBeforeConnect(bc),

		// For this test, we set up every connection to be discarded after a single use.
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

type IAM struct {
	once       *sync.Once
	err        error
	window     time.Duration
	lifetime   time.Duration
	expiration time.Time
	token      string
}

func NewIAM() *IAM {
	return &IAM{
		window:   2 * time.Minute,
		lifetime: 15 * time.Minute,
		once:     &sync.Once{},
	}
}

func (i *IAM) Expired() bool {
	return time.Now().Add(-i.window).After(i.expiration)
}

// Refresh provides IAM-based authentication for AWS RDS.
// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html#UsingWithRDS.IAMDBAuth.Connecting.GoV2
func (i *IAM) Refresh(cfg aws.Config) BeforeConnect {
	return func(ctx context.Context, config *pgx.ConnConfig) error {
		// AWS's recommendation is to reuse the token until it expires. This limits
		// connection throttling.
		if i.Expired() {
			// Every connection attempt will block here, only one will make the call
			// to BuildAuthToken.
			i.once.Do(func() {
				var err error
				var token string

				// Replace i.once so that the next time a connection is attempted and
				// the token is expired, it will attempt to refresh.
				defer func() {
					i.err = err
					i.once = &sync.Once{}
				}()

				endpoint := fmt.Sprintf("%s:%d", config.Host, config.Port)
				token, err = auth.BuildAuthToken(ctx, endpoint, cfg.Region, config.User, cfg.Credentials)
				if err != nil {
					return
				}

				i.token = token
				i.expiration = time.Now().Add(i.lifetime)
			})

			// After the token has been refreshed, if there was an error, return it.
			if i.err != nil {
				return i.err
			}
		}

		config.Password = i.token
		return nil
	}
}
