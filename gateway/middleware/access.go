package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// AccessLog records one structured line per completed request.
//
// The gateway is where every request enters the system, so without this the
// trace for a request reconstructed from its correlation ID begins at whichever
// downstream service happened to log first — and a request rejected at the
// gateway (401, 429) leaves no trace at all, which is exactly the request
// someone will ask about.
//
// It must run inside CorrelationIDMiddleware so the ID is on the context by the
// time the line is written.
func AccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Scrapes would otherwise dominate the log volume at one line every
			// 15 seconds per instance, drowning real traffic.
			if r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			rec := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			// The correlation ID is attached automatically by the logging
			// handler reading it off the context, so it is not passed here.
			logger.InfoContext(r.Context(), "request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote_ip", ClientIP(r),
			)
		})
	}
}

// statusWriter captures the response status, which http.ResponseWriter does not
// expose after the fact.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap keeps http.ResponseController working through the wrapper.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
