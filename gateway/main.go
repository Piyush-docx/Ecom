// Command gateway is the API gateway fronting the e-commerce services.
//
// It terminates client traffic, validates JWTs once, enforces a distributed
// rate limit backed by Redis, and proxies to the backend services. Running
// several instances against one Redis is the point: the limit holds across all
// of them combined rather than per instance.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gateway/middleware"
	"gateway/router"
	rlredis "ratelimiter/redis"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("gateway exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := env("GATEWAY_ADDR", ":8080")
	redisAddr := env("REDIS_ADDR", "localhost:6379")

	// The JWT secret has no default on purpose. A development fallback here
	// would eventually reach production as the real signing key.
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return errors.New("JWT_SECRET must be set")
	}

	limit, err := envInt("RATE_LIMIT", 100)
	if err != nil {
		return err
	}
	window, err := envDuration("RATE_LIMIT_WINDOW", time.Minute)
	if err != nil {
		return err
	}

	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}

	// Token bucket is the default: API traffic is bursty, and absorbing a
	// burst while holding the average rate is the behavior clients expect.
	// See the Phase 8 ADR for when the sliding window counter is the better
	// choice.
	limiter, err := rlredis.NewTokenBucket(rdb, "gateway", limit, window)
	if err != nil {
		return err
	}

	handler, err := router.New(router.Config{
		Limiter: limiter,
		JWT: middleware.JWTConfig{
			Secret:   []byte(secret),
			Issuer:   os.Getenv("JWT_ISSUER"),
			Audience: os.Getenv("JWT_AUDIENCE"),
		},
		Services: map[string]string{
			"auth":    env("AUTH_SERVICE_URL", "http://localhost:8081"),
			"catalog": env("CATALOG_SERVICE_URL", "http://localhost:8082"),
			"orders":  env("ORDERS_SERVICE_URL", "http://localhost:8083"),
			"payment": env("PAYMENT_SERVICE_URL", "http://localhost:8084"),
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: handler,
		// Bounded timeouts keep a slow or idle client from holding a
		// connection open indefinitely.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Shut down gracefully so in-flight requests finish rather than being cut
	// off mid-response when the container is rescheduled.
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

	logger.Info("gateway listening",
		"addr", addr,
		"redis", redisAddr,
		"rate_limit", limit,
		"rate_limit_window", window.String(),
	)

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

func envInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, errors.New(key + " must be an integer: " + v)
	}
	return n, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, errors.New(key + " must be a duration such as 1m: " + v)
	}
	return d, nil
}
