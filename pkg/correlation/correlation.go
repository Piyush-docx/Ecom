// Package correlation carries a request's correlation ID across process
// boundaries.
//
// The gateway assigns an ID to every inbound request (see gateway/middleware).
// Each service must accept that ID, attach it to every log line, and forward it
// on any outbound call — HTTP or Kafka. That chain is what makes Phase 6's
// requirement possible: given one correlation ID, reconstruct the lifecycle of
// a single request across all four services.
//
// This package is deliberately tiny. IMPLEMENTATION_PLAN.md 4 permits shared
// code only for correlation-ID propagation and structured logging; anything
// larger belongs to a service.
package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// Header is the HTTP header carrying the correlation ID. It must match the
// gateway's spelling, since the gateway is what sets it.
const Header = "X-Correlation-ID"

// contextKey is unexported so no other package can collide with or overwrite
// this value. A plain string key would be reachable by anyone.
type contextKey struct{}

// FromContext returns the correlation ID carried by ctx, or "" if there is
// none.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}

// NewContext returns a copy of ctx carrying id.
//
// Exported for work that outlives the request that started it — a Kafka
// consumer in Phase 5 rebuilding a context from a message header, for one.
func NewContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// New returns a random 128-bit correlation ID in hex.
//
// crypto/rand rather than math/rand: IDs reach logs and clients, and
// predictable IDs would let one caller guess another's trace.
func New() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// maxLen bounds an accepted inbound ID. IDs are written to every log line, so
// an unbounded caller-supplied value is both a log-injection vector and a cost
// on every request.
const maxLen = 128

// IsValid reports whether a caller-supplied ID is safe to propagate.
//
// Only printable ASCII excluding space is accepted. Rejecting control
// characters is the point: a newline would let a caller forge entries in a
// service's structured logs, and CR/LF in a header value is a response-
// splitting vector.
func IsValid(id string) bool {
	if id == "" || len(id) > maxLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] <= ' ' || id[i] >= 0x7f {
			return false
		}
	}
	return true
}

// Middleware extracts the correlation ID from an inbound request, or mints one
// if the request arrived without a valid one, and places it on the context.
//
// In normal operation the gateway has already set the header, so a service
// minting its own ID means the request bypassed the gateway — worth noticing
// in logs, but not worth rejecting, since a service must stay debuggable when
// called directly.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(Header)
		if !IsValid(id) {
			id = New()
		}
		w.Header().Set(Header, id)
		next.ServeHTTP(w, r.WithContext(NewContext(r.Context(), id)))
	})
}

// SetHeader copies the context's correlation ID onto an outbound request, so a
// service-to-service call continues the same trace rather than starting a new
// one. It is a no-op when ctx carries no ID.
func SetHeader(ctx context.Context, r *http.Request) {
	if id := FromContext(ctx); id != "" {
		r.Header.Set(Header, id)
	}
}
