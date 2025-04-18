package reconnect

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const dbTokenLifetime = 15 * time.Minute

type credentialsCache interface {
	Invalidate()
}

type IAM struct {
	once       *sync.Once
	err        error
	window     time.Duration
	expiration time.Time
	token      string
	trace      bool
	spanOpts   []tracer.StartSpanOption
}

type IAMOption func(*IAM)

func WithTracing(opts ...tracer.StartSpanOption) IAMOption {
	return func(i *IAM) {
		i.trace = true
		i.spanOpts = opts
	}
}

func NewIAM() *IAM {
	return &IAM{
		window: 2 * time.Minute,
		once:   &sync.Once{},
	}
}

func (i *IAM) NeedsRefresh() bool {
	return time.Now().Add(i.window).After(i.expiration)
}

func (i *IAM) span(ctx context.Context, name string) (context.Context, func()) {
	if !i.trace {
		return ctx, func() {}
	}

	span, ctx := tracer.StartSpanFromContext(ctx, name, i.spanOpts...)
	return ctx, func() {
		opts := []tracer.FinishOption{}
		if i.err != nil {
			opts = append(opts, tracer.WithError(i.err))
		}
		span.Finish(opts...)
	}
}

// GetCredentials provides IAM-based authentication for AWS RDS.
// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html#UsingWithRDS.IAMDBAuth.Connecting.GoV2
func (i *IAM) GetPassword(cfg aws.Config) BeforeConnect {
	return func(ctx context.Context, config *pgx.ConnConfig) error {
		ctx, finish := i.span(ctx, "iam.before_connect")
		defer finish()

		// AWS's recommendation is to reuse the token until it expires. This limits
		// connection throttling.
		if i.NeedsRefresh() {
			refreshCtx, finishRefresh := i.span(ctx, "iam.needs_refresh")
			defer finishRefresh()

			// Every connection attempt will block here, only one will make the call
			// to BuildAuthToken.
			i.once.Do(func() {
				ctx, finish := i.span(refreshCtx, "iam.get_token")
				defer finish()

				var err error
				var token string

				// Replace i.once so that the next time a connection is attempted and
				// the token is expired, it will attempt to refresh.
				defer func() {
					i.err = err
					i.once = &sync.Once{}
				}()

				// Make sure that AWS credentials will be valid for the lifetime of the
				// token.
				var creds aws.Credentials
				creds, err = cfg.Credentials.Retrieve(ctx)
				if err != nil {
					return
				}

				expirationTime := time.Now().Add(dbTokenLifetime)
				if creds.Expires.Before(expirationTime.Add(-1 * time.Minute)) {
					if cache, ok := cfg.Credentials.(credentialsCache); ok {
						cache.Invalidate()
					}
				}

				endpoint := fmt.Sprintf("%s:%d", config.Host, config.Port)
				token, err = auth.BuildAuthToken(ctx, endpoint, cfg.Region, config.User, cfg.Credentials)
				if err != nil {
					return
				}

				i.token = token
				i.expiration = expirationTime
			})

			// After the token has been refreshed, if there was an error, return it.
			if i.err != nil {
				return i.err
			}

			finishRefresh()
		}

		config.Password = i.token
		return nil
	}
}
