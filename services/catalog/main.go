// Command catalog is the product and inventory service.
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

	"pkg/dbx"
	"pkg/logging"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func main() {
	logger := logging.New("catalog", logging.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(logger); err != nil {
		logger.Error("catalog service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := env("CATALOG_ADDR", ":8082")
	dsn := env("CATALOG_DATABASE_URL", "postgres://ecom:ecom@localhost:5432/ecom_catalog?sslmode=disable")

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

	api := &API{store: NewStore(pool), logger: logger}

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
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

	logger.Info("catalog service listening", "addr", addr)
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
