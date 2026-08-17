// Package middleware holds the gateway's HTTP middleware chain.
//
// The chain runs in the order IMPLEMENTATION_PLAN.md Phase 3 prescribes:
//
//	correlation-id -> JWT validation -> rate limiter -> route to service
//
// Each middleware is independent and testable on its own; Router in the router
// package composes them.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"pkg/correlation"
)

// CorrelationIDHeader carries the correlation ID between the client, the
// gateway, and every downstream service.
//
// X-Correlation-ID is not a standard header, but it is the conventional
// spelling and is what the services will look for in Phase 4.
const CorrelationIDHeader = "X-Correlation-ID"

// contextKey is unexported so no other package can collide with the gateway's
// context values. A bare string key would be reachable — and overwritable — by
// any package that happened to use the same literal.
type contextKey int

const (
	correlationIDKey contextKey = iota
	claimsKey
)

// CorrelationID returns the correlation ID carried by ctx, or "" if the
// request did not pass through the CorrelationID middleware.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey).(string)
	return id
}

// ContextWithCorrelationID returns a copy of ctx carrying id.
//
// Exported so downstream code — the Kafka producers in Phase 5, for one — can
// build a context for work that continues after the HTTP request that started
// it has returned.
func ContextWithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationID assigns every request a correlation ID and echoes it back on
// the response.
//
// An inbound X-Correlation-ID is honored so a caller that already has an ID —
// another service, or a client retrying a failed request — keeps one trace
// across the whole call. Anything else gets a fresh random ID.
//
// This is what makes Phase 6's "given one correlation ID, reconstruct the
// lifecycle of one request across four services" possible, so it runs first:
// every later middleware, including rejections, is logged under an ID.
func CorrelationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationIDHeader)
		if !isValidCorrelationID(id) {
			id = newCorrelationID()
		}

		// Echo before calling next: the header must be on the response even if
		// a later middleware rejects the request, or a client could not report
		// which request was refused.
		w.Header().Set(CorrelationIDHeader, id)

		// Stored under both this package's key and pkg/correlation's, because
		// pkg/logging reads the latter to stamp every log line. Without the
		// bridge the gateway's own logs would omit the ID that its downstream
		// services all carry.
		ctx := ContextWithCorrelationID(r.Context(), id)
		ctx = correlation.NewContext(ctx, id)
		// Set it on the outbound request too, so the reverse proxy forwards it
		// downstream without needing to read the context.
		r.Header.Set(CorrelationIDHeader, id)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// maxCorrelationIDLen bounds an accepted inbound ID. Correlation IDs are echoed
// into response headers and logs, so an unbounded caller-supplied value is an
// injection vector into log files and a memory cost on every request.
const maxCorrelationIDLen = 128

// isValidCorrelationID reports whether a caller-supplied ID is safe to reuse.
//
// Only printable ASCII excluding space and DEL is allowed. Rejecting control
// characters matters: a newline in a correlation ID would let a caller forge
// extra lines in the gateway's structured logs, and CR/LF in a header value is
// a response-splitting vector.
func isValidCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] <= ' ' || id[i] >= 0x7f {
			return false
		}
	}
	return true
}

// newCorrelationID returns a random 128-bit ID in hex.
//
// crypto/rand rather than math/rand: IDs appear in logs and are echoed to
// clients, and predictable IDs would let one client guess another's trace.
// crypto/rand.Read never returns an error as of Go 1.24 — it panics internally
// on catastrophic failure — so there is no error path to handle here.
func newCorrelationID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
