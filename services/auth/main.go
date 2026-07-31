// Command auth is the authentication service: it owns user accounts and issues
// the JWTs the gateway validates.
package main

import (
	"context"
	"embed"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"

	"pkg/dbx"
	"pkg/logging"
)

// Migrations are embedded so the binary carries the schema it expects, rather
// than depending on an operator having applied it first.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

func main() {
	logger := logging.New("auth", logging.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(logger); err != nil {
		logger.Error("auth service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := env("AUTH_ADDR", ":8081")
	dsn := env("AUTH_DATABASE_URL", "postgres://ecom:ecom@localhost:5432/ecom_auth?sslmode=disable")

	// No development fallback: a default secret here would eventually become a
	// production signing key. It must also match the gateway's.
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return errors.New("JWT_SECRET must be set")
	}
	if len(secret) < 32 {
		return errors.New("JWT_SECRET must be at least 32 bytes")
	}

	ttl := time.Hour
	if v := os.Getenv("JWT_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return errors.New("JWT_TTL must be a duration such as 1h: " + v)
		}
		ttl = d
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := dbx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := dbx.Migrate(dsn, migrationsFS, "migrations"); err != nil {
		return err
	}
	logger.Info("migrations applied")

	api := &API{
		store:      NewStore(pool),
		logger:     logger,
		jwtSecret:  []byte(secret),
		jwtTTL:     ttl,
		jwtIssuer:  env("JWT_ISSUER", "ecom-auth"),
		bcryptCost: bcrypt.DefaultCost,
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Generous relative to other services: a bcrypt hash at the default
		// cost deliberately takes ~100ms, and a burst of signups queues.
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig

		logger.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
		}
	}()

	logger.Info("auth service listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownDone
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
