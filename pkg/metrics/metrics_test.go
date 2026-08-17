package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// staticRoute returns a fixed route label, standing in for a router's pattern
// lookup.
func staticRoute(label string) RouteFunc {
	return func(*http.Request) string { return label }
}

// serve runs one request through the metrics middleware.
func serve(m *Metrics, route, method, path string, status int) {
	h := m.Middleware(staticRoute(route))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
}

// TestREDMetricsAreRecorded covers the three RED signals Phase 6 requires:
// request rate, error rate, and duration.
func TestREDMetricsAreRecorded(t *testing.T) {
	m := New("test")

	serve(m, "/orders/{id}", http.MethodGet, "/orders/abc", http.StatusOK)
	serve(m, "/orders/{id}", http.MethodGet, "/orders/def", http.StatusOK)
	serve(m, "/orders/{id}", http.MethodGet, "/orders/ghi", http.StatusInternalServerError)

	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET", "/orders/{id}", "200")); got != 2 {
		t.Errorf("200 responses counted = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET", "/orders/{id}", "500")); got != 1 {
		t.Errorf("500 responses counted = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.errors.WithLabelValues("GET", "/orders/{id}")); got != 1 {
		t.Errorf("errors counted = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.duration); got == 0 {
		t.Error("no duration observations were recorded")
	}
}

// TestClientErrorsAreNotServiceErrors confirms 4xx does not inflate the error
// rate.
//
// This matters for alerting: an error-rate alert that counts 4xx fires when
// users mistype a password, which trains everyone to ignore it — and then the
// real outage is ignored too.
func TestClientErrorsAreNotServiceErrors(t *testing.T) {
	m := New("test")

	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusTooManyRequests} {
		serve(m, "/orders/{id}", http.MethodGet, "/orders/x", status)
	}

	if got := testutil.ToFloat64(m.errors.WithLabelValues("GET", "/orders/{id}")); got != 0 {
		t.Errorf("4xx responses counted as service errors = %v, want 0", got)
	}

	// They are still counted as requests, so the rate is complete.
	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET", "/orders/{id}", "404")); got != 1 {
		t.Errorf("404 requests counted = %v, want 1", got)
	}
}

// TestRouteLabelIsThePatternNotThePath is the cardinality guard.
//
// Every distinct label value is a new Prometheus time series. Labelling by the
// concrete path would let one client with a loop of unique order IDs create
// unbounded series and exhaust the monitoring system — taking down the thing
// that is supposed to tell you what went wrong.
func TestRouteLabelIsThePatternNotThePath(t *testing.T) {
	m := New("test")

	// A thousand distinct paths, one route pattern.
	for i := 0; i < 1000; i++ {
		serve(m, "/orders/{id}", http.MethodGet, "/orders/order-"+strings.Repeat("x", i%50), http.StatusOK)
	}

	if got := testutil.CollectAndCount(m.requests); got != 1 {
		t.Errorf("1000 distinct paths produced %d time series, want 1 — "+
			"the route label is leaking the concrete path and cardinality is unbounded", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET", "/orders/{id}", "200")); got != 1000 {
		t.Errorf("requests counted = %v, want 1000", got)
	}
}

// TestMetricsEndpointIsNotMeasured confirms scrapes do not pollute the metrics
// they are scraping.
func TestMetricsEndpointIsNotMeasured(t *testing.T) {
	m := New("test")

	h := m.Middleware(staticRoute("/metrics"))(m.Handler())
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/metrics", nil))
	}

	if got := testutil.CollectAndCount(m.requests); got != 0 {
		t.Errorf("the /metrics endpoint recorded %d request series, want 0 — "+
			"a 15s scrape interval would dominate the request rate", got)
	}
}

// TestHandlerExposesMetrics confirms the endpoint serves the registry in
// Prometheus text format, including the constant service label.
func TestHandlerExposesMetrics(t *testing.T) {
	m := New("orders")
	serve(m, "/orders/{id}", http.MethodGet, "/orders/abc", http.StatusOK)

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, want := range []string{
		"http_requests_total",
		"http_request_duration_seconds",
		`service="orders"`,
		// Runtime metrics, which distinguish "slow" from "leaking" under load.
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the metrics output does not contain %q", want)
		}
	}
}

// TestRateLimitMetrics covers the allowed/denied pair Phase 6 asks for.
func TestRateLimitMetrics(t *testing.T) {
	m := New("gateway")

	for i := 0; i < 7; i++ {
		m.RecordRateLimitDecision("token_bucket", true, 10-i)
	}
	for i := 0; i < 3; i++ {
		m.RecordRateLimitDecision("token_bucket", false, 0)
	}

	if got := testutil.ToFloat64(m.rateLimitDecisions.WithLabelValues("token_bucket", "allowed")); got != 7 {
		t.Errorf("allowed = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.rateLimitDecisions.WithLabelValues("token_bucket", "denied")); got != 3 {
		t.Errorf("denied = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.rateLimitRemaining.WithLabelValues("token_bucket")); got != 0 {
		t.Errorf("remaining gauge = %v, want the most recent value 0", got)
	}
}

// TestSagaMetrics covers the event and outcome counters that make the saga
// observable — otherwise a stuck consumer is invisible until orders pile up.
func TestSagaMetrics(t *testing.T) {
	m := New("orders")

	m.RecordEventPublished("order.created", "OrderCreated")
	m.RecordEventConsumed("payment.succeeded", "PaymentSucceeded", "success")
	m.RecordEventConsumed("payment.failed", "PaymentFailed", "retry")
	m.RecordSagaOutcome("confirmed")
	m.RecordSagaOutcome("compensated")

	if got := testutil.ToFloat64(m.eventsPublished.WithLabelValues("order.created", "OrderCreated")); got != 1 {
		t.Errorf("published = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.eventsConsumed.WithLabelValues("payment.failed", "PaymentFailed", "retry")); got != 1 {
		t.Errorf("retry outcome = %v, want 1 — a consumer stuck retrying must be visible", got)
	}
	if got := testutil.ToFloat64(m.sagaOutcomes.WithLabelValues("compensated")); got != 1 {
		t.Errorf("compensated = %v, want 1", got)
	}
}

// TestStatusRecorderDefaultsTo200 covers a handler that writes a body without
// calling WriteHeader, which Go treats as 200.
func TestStatusRecorderDefaultsTo200(t *testing.T) {
	m := New("test")

	h := m.Middleware(staticRoute("/healthz"))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET", "/healthz", "200")); got != 1 {
		t.Errorf("implicit 200 counted = %v, want 1", got)
	}
}
