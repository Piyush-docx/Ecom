package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"pkg/correlation"
)

// capture returns a logger writing JSON into buf, mirroring New's handler
// chain so the tests exercise the real correlation behaviour.
func capture(buf *bytes.Buffer, service string) *slog.Logger {
	base := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(&correlationHandler{inner: base}).With("service", service)
}

// decode parses the captured lines.
func decode(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %v (line=%q)", err, line)
		}
		out = append(out, m)
	}
	return out
}

// TestCorrelationIDIsStampedAutomatically is the mechanism behind Phase 6's
// acceptance criterion.
//
// The handler reads the ID off the context rather than requiring each call site
// to pass it. That matters because a single forgotten call site is a hole in
// the trace at exactly the point someone is trying to debug — and the missing
// line looks identical to "this code never ran".
func TestCorrelationIDIsStampedAutomatically(t *testing.T) {
	var buf bytes.Buffer
	logger := capture(&buf, "orders")

	ctx := correlation.NewContext(context.Background(), "trace-abc-123")
	logger.InfoContext(ctx, "order created", "order_id", "o-1")

	lines := decode(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1", len(lines))
	}

	if got := lines[0]["correlation_id"]; got != "trace-abc-123" {
		t.Errorf("correlation_id = %v, want the context's id — the trace breaks here", got)
	}
	if got := lines[0]["service"]; got != "orders" {
		t.Errorf("service = %v, want orders", got)
	}
	if got := lines[0]["msg"]; got != "order created" {
		t.Errorf("msg = %v, want the message", got)
	}
}

// TestLogsWithoutCorrelationAreStillEmitted confirms a context with no ID does
// not lose the line entirely. Startup and shutdown logs have no request behind
// them, and silently dropping them would hide a service failing to boot.
func TestLogsWithoutCorrelationAreStillEmitted(t *testing.T) {
	var buf bytes.Buffer
	logger := capture(&buf, "auth")

	logger.InfoContext(context.Background(), "service listening", "addr", ":8081")

	lines := decode(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1", len(lines))
	}
	if _, present := lines[0]["correlation_id"]; present {
		t.Error("a line with no correlation id in context carries an empty correlation_id field")
	}
	if got := lines[0]["msg"]; got != "service listening" {
		t.Errorf("msg = %v", got)
	}
}

// TestCorrelationSurvivesWithAttrsAndGroup confirms the handler keeps working
// through slog's With and WithGroup, which return new handlers.
//
// A wrapper that forgets to rewrap in those methods loses the correlation ID
// for every logger derived with .With(...) — which is most of them in practice.
func TestCorrelationSurvivesWithAttrsAndGroup(t *testing.T) {
	var buf bytes.Buffer
	logger := capture(&buf, "catalog")

	ctx := correlation.NewContext(context.Background(), "trace-xyz")

	derived := logger.With("component", "store")
	derived.InfoContext(ctx, "query executed")

	lines := decode(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	if got := lines[0]["correlation_id"]; got != "trace-xyz" {
		t.Errorf("a logger derived with .With lost the correlation id: got %v", got)
	}
	if got := lines[0]["component"]; got != "store" {
		t.Errorf("component = %v, want store", got)
	}
}

// TestOneCorrelationIDSpansEveryService is the Phase 6 acceptance criterion in
// miniature:
//
//	"given only the correlation ID from one order request, you can grep/query
//	 logs across all four services and see the full lifecycle of that one
//	 request."
//
// Each service logs into a shared buffer, standing in for aggregated logs.
// Filtering by one ID must yield the whole ordered story and nothing else.
func TestOneCorrelationIDSpansEveryService(t *testing.T) {
	var buf bytes.Buffer

	const traced = "trace-the-one-we-want"
	const other = "trace-unrelated-traffic"

	tracedCtx := correlation.NewContext(context.Background(), traced)
	otherCtx := correlation.NewContext(context.Background(), other)

	// The lifecycle of one checkout, as the five processes would emit it.
	capture(&buf, "gateway").InfoContext(tracedCtx, "request completed")
	capture(&buf, "catalog").InfoContext(tracedCtx, "stock reserved")
	capture(&buf, "orders").InfoContext(tracedCtx, "order created")
	capture(&buf, "orders").InfoContext(tracedCtx, "event published")
	capture(&buf, "payment").InfoContext(tracedCtx, "event received")
	capture(&buf, "payment").InfoContext(tracedCtx, "charge attempted via saga")
	capture(&buf, "orders").InfoContext(tracedCtx, "order confirmed by saga")

	// Concurrent unrelated traffic, which the filter must exclude.
	capture(&buf, "gateway").InfoContext(otherCtx, "request completed")
	capture(&buf, "auth").InfoContext(otherCtx, "user signed in")

	var matched []map[string]any
	for _, line := range decode(t, &buf) {
		if line["correlation_id"] == traced {
			matched = append(matched, line)
		}
	}

	if len(matched) != 7 {
		t.Fatalf("filtering by one correlation id matched %d lines, want 7", len(matched))
	}

	// Every service involved in the checkout must be represented, or the trace
	// has a hole where a hop should be.
	seen := map[string]bool{}
	for _, line := range matched {
		seen[line["service"].(string)] = true
	}
	for _, want := range []string{"gateway", "catalog", "orders", "payment"} {
		if !seen[want] {
			t.Errorf("the trace has no line from %s — the lifecycle is incomplete", want)
		}
	}

	// And no unrelated traffic leaked in.
	for _, line := range matched {
		if line["correlation_id"] != traced {
			t.Errorf("the filter admitted an unrelated line: %v", line)
		}
	}
}

// TestParseLevel confirms an unrecognised level falls back to Info rather than
// silencing the service, so a typo in configuration cannot hide an outage.
func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"DEBUG":    slog.LevelDebug,
		"warn":     slog.LevelWarn,
		"error":    slog.LevelError,
		"info":     slog.LevelInfo,
		"":         slog.LevelInfo,
		"nonsense": slog.LevelInfo,
		"VERBOSE":  slog.LevelInfo,
	}
	for input, want := range cases {
		if got := ParseLevel(input); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", input, got, want)
		}
	}
}
