package middleware

import (
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"pkg/metrics"
	"ratelimiter"
)

// Rate limit response headers.
//
// The X-RateLimit-* family is a de facto convention rather than a standard
// (RFC 9331 proposes RateLimit-* instead, but the X- spelling is what clients
// and SDKs actually look for today). Retry-After is standard, defined by
// RFC 9110 for 429 and 503 responses.
const (
	RateLimitLimitHeader     = "X-RateLimit-Limit"
	RateLimitRemainingHeader = "X-RateLimit-Remaining"
	RateLimitResetHeader     = "X-RateLimit-Reset"
	RetryAfterHeader         = "Retry-After"
)

// KeyFunc derives the rate-limit key for a request.
//
// The key decides who shares a budget with whom, so it is the single most
// consequential rate-limiting decision. Getting it wrong either lets one
// attacker exhaust everyone's budget, or gives every attacker their own.
type KeyFunc func(*http.Request) string

// KeyBySubjectOrIP keys authenticated requests by the JWT subject and
// unauthenticated ones by client IP.
//
// Keying authenticated traffic by IP would throttle every user behind one NAT
// or corporate proxy as a single client. Keying by subject follows the account
// rather than the connection, which is what a per-user quota means.
//
// The prefixes keep the two namespaces disjoint, so a user whose ID happens to
// look like an IP address cannot collide with one.
func KeyBySubjectOrIP(r *http.Request) string {
	if claims, ok := ClaimsFromContext(r.Context()); ok && claims.Subject != "" {
		return "user:" + claims.Subject
	}
	return "ip:" + ClientIP(r)
}

// ClientIP extracts the client address from a request.
//
// It deliberately ignores X-Forwarded-For. That header is caller-controlled:
// anyone can send an arbitrary value and, if trusted blindly, mint themselves
// an unlimited number of distinct rate-limit keys — which defeats the limiter
// entirely. Honoring it requires knowing how many proxies sit in front of the
// gateway and trusting exactly that many hops; that belongs in Phase 7 when the
// real load-balancer topology exists, not guessed at here.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// RemoteAddr without a port (rare, but possible behind some
		// transports) is still usable as an identity.
		return r.RemoteAddr
	}
	return host
}

// RateLimitConfig configures RateLimit.
type RateLimitConfig struct {
	// Limiter decides whether a request proceeds. Any implementation works —
	// the in-memory algorithms or the Redis-backed ones — because both satisfy
	// ratelimiter.Limiter.
	Limiter ratelimiter.Limiter

	// KeyFunc derives the key. Defaults to KeyBySubjectOrIP.
	KeyFunc KeyFunc

	// FailOpen decides what happens when the limiter itself errors, which in
	// practice means Redis is unreachable.
	//
	// False (the default) fails closed: a limiter outage rejects traffic with
	// 503. True fails open: traffic is allowed through unlimited.
	//
	// Neither is universally right. Failing closed turns a Redis outage into a
	// full outage; failing open turns it into an unprotected backend. The
	// default is closed because this gateway fronts a payment path, where
	// serving nothing beats serving an unbounded charge rate.
	FailOpen bool

	// Logger records limiter failures. Defaults to slog.Default().
	Logger *slog.Logger

	// Metrics, when set, records allowed/denied counts and the remaining
	// allowance. Optional so tests can construct the middleware without it.
	Metrics *metrics.Metrics

	// Algorithm labels the metrics, so a deployment that switches algorithms
	// can be compared against its predecessor.
	Algorithm string
}

// RateLimit enforces the limiter and sets rate-limit headers on every response,
// allowed or denied, as IMPLEMENTATION_PLAN.md Phase 3 requires.
//
// Setting the headers on allowed responses too is what lets a well-behaved
// client pace itself rather than discovering the limit by being refused.
func RateLimit(cfg RateLimitConfig) func(http.Handler) http.Handler {
	keyFn := cfg.KeyFunc
	if keyFn == nil {
		keyFn = KeyBySubjectOrIP
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	algorithm := cfg.Algorithm
	if algorithm == "" {
		algorithm = "unknown"
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFn(r)

			res, err := cfg.Limiter.Allow(r.Context(), key)
			if err != nil {
				logger.ErrorContext(r.Context(), "rate limiter unavailable",
					"error", err,
					"key", key,
					"correlation_id", CorrelationID(r.Context()),
					"fail_open", cfg.FailOpen,
				)
				if !cfg.FailOpen {
					// No rate-limit headers here: their values would be
					// fabricated, and a client pacing itself against invented
					// numbers is worse off than one that sees none.
					writeError(w, r, http.StatusServiceUnavailable, "rate limiter unavailable")
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			if cfg.Metrics != nil {
				cfg.Metrics.RecordRateLimitDecision(algorithm, res.Allowed, res.Remaining)
			}

			setRateLimitHeaders(w, res)

			if !res.Allowed {
				w.Header().Set(RetryAfterHeader, retryAfterSeconds(res.RetryAfter))
				writeError(w, r, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// setRateLimitHeaders writes the X-RateLimit-* family.
func setRateLimitHeaders(w http.ResponseWriter, res ratelimiter.Result) {
	h := w.Header()
	h.Set(RateLimitLimitHeader, strconv.Itoa(res.Limit))
	h.Set(RateLimitRemainingHeader, strconv.Itoa(res.Remaining))
	h.Set(RateLimitResetHeader, strconv.Itoa(secondsCeil(res.ResetAfter)))
}

// retryAfterSeconds renders a Retry-After value.
//
// RFC 9110 defines Retry-After as either an HTTP-date or a non-negative integer
// number of seconds; fractional seconds are not valid. A sub-second wait must
// therefore round up to 1 rather than down to 0, since 0 would invite an
// immediate retry that is certain to be refused again.
func retryAfterSeconds(d time.Duration) string {
	s := secondsCeil(d)
	if s < 1 {
		s = 1
	}
	return strconv.Itoa(s)
}

// secondsCeil converts a duration to whole seconds, rounding up so an advertised
// wait is never shorter than the real one.
func secondsCeil(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}
