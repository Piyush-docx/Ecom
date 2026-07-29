package router

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"gateway/middleware"
	"ratelimiter"
)

// Config configures the gateway's router.
type Config struct {
	// Limiter enforces the rate limit. Required.
	Limiter ratelimiter.Limiter

	// JWT configures token validation on protected routes. Required.
	JWT middleware.JWTConfig

	// Services maps a route prefix to the upstream base URL serving it, e.g.
	// {"catalog": "http://catalog:8080"}. Prefixes correspond to the four
	// services in IMPLEMENTATION_PLAN.md Phase 4.
	Services map[string]string

	// FailOpen controls behavior when the limiter errors. See
	// middleware.RateLimitConfig.FailOpen; the default (false) fails closed.
	FailOpen bool

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// New builds the gateway's HTTP handler.
//
// The middleware order follows IMPLEMENTATION_PLAN.md Phase 3:
//
//	correlation-id -> JWT validation -> rate limiter -> route to service
//
// Rate limiting sits after JWT deliberately, so an authenticated request is
// keyed by user rather than by IP. The tradeoff is that invalid tokens are
// rejected without consuming rate-limit budget, which means the JWT
// verification path itself is not protected by the limiter. HMAC verification
// is cheap enough that this is a reasonable trade, but it is the kind of
// decision worth revisiting if the gateway is ever exposed to untrusted
// traffic at scale — one for the Phase 8 ADRs.
func New(cfg Config) (http.Handler, error) {
	if cfg.Limiter == nil {
		return nil, fmt.Errorf("router: Limiter is required")
	}
	if err := cfg.JWT.Validate(); err != nil {
		return nil, fmt.Errorf("router: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	proxies := make(map[string]http.Handler, len(cfg.Services))
	for name, raw := range cfg.Services {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("router: service %q has an invalid URL %q: %w", name, raw, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("router: service %q URL %q must be absolute (scheme and host)", name, raw)
		}
		proxies[name] = newProxy(u, logger)
	}

	rateLimit := middleware.RateLimit(middleware.RateLimitConfig{
		Limiter:  cfg.Limiter,
		FailOpen: cfg.FailOpen,
		Logger:   logger,
	})

	r := chi.NewRouter()

	// Runs before everything: a client must not be able to assert identity.
	r.Use(stripUntrustedHeaders)
	r.Use(middleware.CorrelationIDMiddleware)

	// Health checks are deliberately outside the rate limiter and auth. A
	// load balancer polling health must not consume anyone's quota, and must
	// keep working when Redis is down — otherwise a limiter outage would make
	// every gateway instance look unhealthy and be pulled from rotation,
	// turning a degradation into a total outage.
	r.Get("/healthz", health)

	// Public routes: no JWT, so the limiter keys them by IP. Signup and login
	// cannot require the token they exist to issue.
	r.Group(func(pub chi.Router) {
		pub.Use(rateLimit)
		if p, ok := proxies["auth"]; ok {
			pub.Handle("/auth/*", p)
		}
	})

	// Protected routes: JWT first, so the limiter keys by user.
	r.Group(func(prot chi.Router) {
		prot.Use(middleware.RequireJWT(cfg.JWT))
		prot.Use(rateLimit)
		for _, name := range []string{"catalog", "orders", "payment"} {
			if p, ok := proxies[name]; ok {
				prot.Handle("/"+name+"/*", p)
			}
		}
	})

	return r, nil
}

// health reports gateway liveness. It intentionally does not check Redis or
// the upstreams: this answers "is this process able to serve requests", which
// is what a load balancer needs to decide whether to route to it.
func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
