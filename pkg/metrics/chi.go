package metrics

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// ChiRoute returns the route pattern chi matched for a request, for use as the
// metrics route label.
//
// chi records the matched pattern ("/orders/{id}") rather than the concrete
// path ("/orders/abc-123"), which is exactly the low-cardinality label the
// middleware needs — see RouteFunc for why the distinction matters.
//
// The pattern is only populated after routing, so this must be read inside the
// handler chain rather than before it. Unmatched requests fall back to a single
// literal so a scan for nonexistent paths cannot create one series per probe.
func ChiRoute(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return "unmatched"
}
