// Package dbx holds the database conventions every service shares: connecting
// with sane pool limits, and applying that service's own migrations at startup.
//
// Each service owns its schema and its migrations. This package supplies the
// mechanism, never the schema — IMPLEMENTATION_PLAN.md 1.3 requires no shared
// tables, so nothing here knows what any service's tables look like.
package dbx

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a connection pool and verifies it can reach the database.
//
// It retries briefly: in docker compose a service can start before Postgres is
// accepting connections, and failing immediately would make startup order a
// coin flip. The compose healthcheck covers the common case, but a service
// restarting on its own does not get that guarantee.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing database DSN: %w", err)
	}

	// Bounded pool: without a cap, a burst of concurrent requests would open
	// connections until Postgres refuses them, turning a traffic spike into a
	// database outage.
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = pool.Ping(pingCtx)
		cancel()
		if lastErr == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	pool.Close()
	return nil, fmt.Errorf("database unreachable after 10 attempts: %w", lastErr)
}

// Migrate applies a service's migrations, which are embedded in its binary so a
// deployed service carries the schema it expects rather than depending on an
// operator having run something first.
//
// dir is the path within fsys holding the numbered .sql files.
func Migrate(dsn string, fsys fs.FS, dir string) error {
	src, err := iofs.New(fsys, dir)
	if err != nil {
		return fmt.Errorf("reading migrations from %s: %w", dir, err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+trimScheme(dsn))
	if err != nil {
		return fmt.Errorf("preparing migrations: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("applying migrations: %w", err)
	}
	return nil
}

// trimScheme strips a postgres:// or postgresql:// prefix.
//
// golang-migrate selects its database driver from the URL scheme, so the DSN
// must be re-prefixed with pgx5:// to use the pgx/v5 driver rather than the
// default postgres one. Everything after the scheme is unchanged.
func trimScheme(dsn string) string {
	for _, prefix := range []string{"postgres://", "postgresql://", "pgx5://", "pgx://"} {
		if len(dsn) >= len(prefix) && dsn[:len(prefix)] == prefix {
			return dsn[len(prefix):]
		}
	}
	return dsn
}

// Ensure the pgx migrate driver is linked in; it registers itself on import.
var _ = pgx.ErrNilConfig
