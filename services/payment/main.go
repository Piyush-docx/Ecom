// Command payment is the payment service. It is the saga participant in Phase
// 5: it consumes OrderCreated, attempts a charge exactly once per order, and
// reports the outcome back.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"pkg/dbx"
	"pkg/events"
	"pkg/logging"
	"pkg/metrics"
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

	store := NewStore(pool)
	gateway := StubGateway{FailAmountCents: failAmount}

	m := metrics.New("payment")

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	if err := events.EnsureTopics(ctx, brokers, events.SagaTopics, events.DefaultPartitions, 1); err != nil {
		return fmt.Errorf("creating kafka topics: %w", err)
	}
	if err := events.WaitForTopics(ctx, brokers, events.SagaTopics, 30*time.Second); err != nil {
		return fmt.Errorf("waiting for kafka topics: %w", err)
	}
	logger.Info("kafka topics ready", "brokers", brokers)

	publisher := events.NewPublisher(brokers, logger)
	defer publisher.Close()

	saga := NewSagaConsumer(store, gateway, publisher, m, logger)

	// Consume OrderCreated. The group is payment-specific, so payment and
	// orders each see every message on their respective topics.
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	defer stopConsumer()

	go func() {
		c := events.NewConsumer(brokers, events.TopicOrderCreated, "payment-service",
			saga.HandleOrderCreated, logger)
		if err := c.Run(consumerCtx); err != nil {
			logger.Error("consumer stopped with an error", "error", err)
		}
	}()

	api := &API{
		store:   store,
		gateway: gateway,
		metrics: m,
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
