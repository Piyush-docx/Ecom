// Command payment is the payment service. It is the saga participant in Phase
// 5: it consumes OrderCreated, attempts a charge exactly once per order, and
// reports the outcome back.
package main

import (
	"context"
	"embed"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"pkg/dbx"
	"pkg/logging"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func main() {
	logger := logging.New("payment", logging.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(logger); err != nil {
		logger.Error("payment service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := env("PAYMENT_ADDR", ":8084")
	dsn := env("PAYMENT_DATABASE_URL", "postgres://ecom:ecom@localhost:5432/ecom_payment?sslmode=disable")

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

	// A stub processor stands in for Stripe, per IMPLEMENTATION_PLAN.md Phase 4.
	// PAYMENT_FAIL_AMOUNT_CENTS makes failure deterministic rather than random,
	// so Phase 5 can test the compensating path on demand.
	var failAmount int64
	if v := os.Getenv("PAYMENT_FAIL_AMOUNT_CENTS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return errors.New("PAYMENT_FAIL_AMOUNT_CENTS must be an integer: " + v)
		}
		failAmount = n
	}

	api := &API{
		store:   NewStore(pool),
		gateway: StubGateway{FailAmountCents: failAmount},
		logger:  logger,
	}

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

	logger.Info("payment service listening", "addr", addr)
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
