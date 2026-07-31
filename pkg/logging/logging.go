// Package logging provides the structured JSON logger every service shares.
//
// IMPLEMENTATION_PLAN.md Phase 6 requires structured logs everywhere, with the
// correlation ID present on every line. Rather than asking each call site to
// remember to attach it, the handler here pulls it off the context
// automatically — a log line that silently loses its correlation ID is a line
// that cannot be found later, which defeats the purpose.
package logging

import (
	"context"
	"log/slog"
	"os"

	"pkg/correlation"
)

// New returns a JSON logger that stamps every record with the service name and,
// when present, the context's correlation ID.
//
// level accepts slog's usual levels; callers typically pass slog.LevelInfo.
func New(service string, level slog.Level) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(&correlationHandler{inner: base}).With("service", service)
}

// correlationHandler wraps a slog.Handler and adds the correlation ID from the
// context to every record.
type correlationHandler struct {
	inner slog.Handler
}

func (h *correlationHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *correlationHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := correlation.FromContext(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.inner.Handle(ctx, r)
}

func (h *correlationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &correlationHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *correlationHandler) WithGroup(name string) slog.Handler {
	return &correlationHandler{inner: h.inner.WithGroup(name)}
}

// ParseLevel converts a level name to a slog.Level, defaulting to Info for an
// empty or unrecognized value so a typo in configuration cannot silence logs.
func ParseLevel(s string) slog.Level {
	switch s {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "warn", "WARN":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
