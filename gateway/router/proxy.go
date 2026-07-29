// Package router composes the gateway's middleware chain and routes requests to
// the backend services.
package router

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"gateway/middleware"
)

// hopByHopHeaders are meaningful only for a single transport connection and
// must not be forwarded to an upstream, per RFC 9110 7.6.1. httputil's
// ReverseProxy already strips these, but Connection-named headers are stripped
// explicitly below because a client can nominate arbitrary headers there.
var hopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// newProxy returns a reverse proxy to target.
//
// The gateway is the only thing that may assert a user's identity downstream,
// so the proxy strips any inbound X-User-ID before the JWT middleware's value
// is applied. Without that, a client could set the header itself and, on a
// public route where RequireJWT never runs to overwrite it, impersonate anyone.
func newProxy(target *url.URL, logger *slog.Logger) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			// Preserve the client's Host for upstream logging and routing.
			pr.Out.Host = pr.In.Host

			// SetXForwarded sets X-Forwarded-For/-Host/-Proto from the actual
			// connection, replacing anything the client sent. Note the rate
			// limiter deliberately does not consult these; see ClientIP.
			pr.SetXForwarded()

			for _, h := range hopByHopHeaders {
				pr.Out.Header.Del(h)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.ErrorContext(r.Context(), "upstream request failed",
				"error", err,
				"upstream", target.String(),
				"path", r.URL.Path,
				"correlation_id", middleware.CorrelationID(r.Context()),
			)
			// 502 rather than 500: the gateway itself is healthy; the upstream
			// is not.
			http.Error(w, `{"error":"upstream unavailable"}`, http.StatusBadGateway)
		},
		// A bounded upstream timeout keeps one slow service from exhausting the
		// gateway's connections.
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
}

// stripUntrustedHeaders removes headers only the gateway may set, before any
// middleware runs.
//
// SubjectHeader is the critical one: services trust it as proof of identity
// (IMPLEMENTATION_PLAN.md 1.7), so a client-supplied value must never survive.
// RequireJWT overwrites it on protected routes, but public routes have no JWT
// middleware to do that — hence stripping unconditionally here.
func stripUntrustedHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del(middleware.SubjectHeader)
		next.ServeHTTP(w, r)
	})
}
