// Command orders is the order service. It is the saga initiator in Phase 5:
// it creates a pending order, and confirms or cancels it based on what payment
// reports back.
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
	logger := logging.New("orders", logging.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(logger); err != nil {
		logger.Error("orders service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := env("ORDERS_ADDR", ":8083")
	dsn := env("ORDERS_DATABASE_URL", "postgres://ecom:ecom@localhost:5432/ecom_orders?sslmode=disable")

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

	store := NewStore(pool)
	catalog := NewCatalogClient(env("CATALOG_SERVICE_URL", "http://localhost:8082"))

	m := metrics.New("orders")

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")

	// Create the saga's topics before producing or consuming. Auto-creation
	// races the first produce, which fails with "Unknown Topic Or Partition"
	// while metadata propagates -- so the very first order of a cold start
	// would be lost.
	if err := events.EnsureTopics(ctx, brokers, events.SagaTopics, events.DefaultPartitions, 1); err != nil {
		return fmt.Errorf("creating kafka topics: %w", err)
	}
	if err := events.WaitForTopics(ctx, brokers, events.SagaTopics, 30*time.Second); err != nil {
		return fmt.Errorf("waiting for kafka topics: %w", err)
	}
	logger.Info("kafka topics ready", "brokers", brokers)

	publisher := events.NewPublisher(brokers, logger)
	defer publisher.Close()

	saga := NewSagaCoordinator(store, catalog, publisher, m, logger)

	api := &API{
		store:   store,
		catalog: catalog,
		saga:    saga,
		metrics: m,
		logger:  logger,
	}

	// Consume payment outcomes. Both consumers share this service's group, so
	// each message is handled once across however many instances run.
	consumerCtx, stopConsumers := context.WithCancel(context.Background())
	defer stopConsumers()

	consumers := []*events.Consumer{
		events.NewConsumer(brokers, events.TopicPaymentSucceeded, "orders-service",
			saga.HandlePaymentSucceeded, logger),
		events.NewConsumer(brokers, events.TopicPaymentFailed, "orders-service",
			saga.HandlePaymentFailed, logger),
	}
	for _, c := range consumers {
		go func(c *events.Consumer) {
			if err := c.Run(consumerCtx); err != nil {
				logger.Error("consumer stopped with an error", "error", err)
			}
		}(c)
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

	logger.Info("orders service listening", "addr", addr)
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
