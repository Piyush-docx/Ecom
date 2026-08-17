// Package metrics provides the RED metrics every service exposes, plus the
// domain-specific metrics the rate limiter and the saga need.
//
// RED — Rate, Errors, Duration — is the standard service-level triple:
// how many requests, how many failed, and how long they took. Those three
// answer "is this service healthy" for any request-driven component, which is
// what IMPLEMENTATION_PLAN.md Phase 6 asks for.
//
// # Why a per-service registry rather than the global default
//
// Each service constructs its own Registry. Using prometheus.DefaultRegisterer
// would make metric registration a process-global side effect: two services in
// one test binary would panic on duplicate registration, and a test could not
// assert on a clean set of counters. An explicit registry makes the dependency
// visible and the tests deterministic.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds one service's instruments.
type Metrics struct {
	registry *prometheus.Registry

	// RED metrics.
	requests *prometheus.CounterVec
	errors   *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge

	// Rate limiter metrics.
	rateLimitDecisions *prometheus.CounterVec
	rateLimitRemaining *prometheus.GaugeVec

	// Saga metrics.
	eventsPublished *prometheus.CounterVec
	eventsConsumed  *prometheus.CounterVec
	sagaOutcomes    *prometheus.CounterVec
}

// New returns the metric set for a service, registered on its own registry.
func New(service string) *Metrics {
	reg := prometheus.NewRegistry()

	// Go runtime and process collectors: goroutine counts, GC pauses, memory,
	// file descriptors. During Phase 7's load test these distinguish "the
	// service is slow" from "the service is out of memory or leaking
	// goroutines", which the RED metrics alone cannot.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// A constant service label on every metric, so a single Prometheus scraping
	// all five processes can still separate them.
	labels := prometheus.Labels{"service": service}
	factory := prometheus.WrapRegistererWith(labels, reg)

	m := &Metrics{
		registry: reg,

		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests handled, by method, route and status code.",
		}, []string{"method", "route", "status"}),

		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_request_errors_total",
			Help: "HTTP requests that returned 5xx, by method and route.",
		}, []string{"method", "route"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "HTTP request latency in seconds, by method and route.",
			// DefBuckets spans 5ms to 10s, which brackets everything here:
			// a cached catalog read sits near the bottom, and a bcrypt hash at
			// the default cost (~100ms by design) sits in the middle.
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),

		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		}),

		rateLimitDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_decisions_total",
			Help: "Rate limiter decisions, by algorithm and outcome (allowed or denied).",
		}, []string{"algorithm", "decision"}),

		rateLimitRemaining: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ratelimit_remaining_tokens",
			Help: "Remaining allowance observed on the most recent decision, sampled by algorithm.",
		}, []string{"algorithm"}),

		eventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_events_published_total",
			Help: "Saga events published, by topic and event type.",
		}, []string{"topic", "type"}),

		eventsConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_events_consumed_total",
			Help: "Saga events consumed, by topic, event type and handling outcome.",
		}, []string{"topic", "type", "outcome"}),

		sagaOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_order_outcomes_total",
			Help: "Terminal saga outcomes per order, by outcome (confirmed or compensated).",
		}, []string{"outcome"}),
	}

	factory.MustRegister(
		m.requests, m.errors, m.duration, m.inFlight,
		m.rateLimitDecisions, m.rateLimitRemaining,
		m.eventsPublished, m.eventsConsumed, m.sagaOutcomes,
	)

	return m
}

// Handler returns the /metrics HTTP handler for this service's registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// Registry exposes the underlying registry, for tests that need to gather
// metrics directly rather than scrape them.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// statusRecorder captures the status code, which http.ResponseWriter does not
// expose after the fact.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	// A handler that writes without calling WriteHeader implies 200.
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer, so wrapping
// does not break flushing or hijacking for any handler that needs them.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// RouteFunc derives the route label for a request.
//
// It must return the route *pattern* ("/orders/{id}"), never the concrete path
// ("/orders/abc-123"). Every distinct label value creates a new time series, so
// labelling by raw path would let one client generate unbounded cardinality and
// exhaust Prometheus's memory — the classic way monitoring takes down the thing
// it monitors.
type RouteFunc func(*http.Request) string

// Middleware records RED metrics for every request.
func (m *Metrics) Middleware(route RouteFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The metrics endpoint itself is not measured: scraping every 15s
			// would otherwise dominate the request rate and skew the latency
			// histogram with work nobody cares about.
			if r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			m.inFlight.Inc()
			defer m.inFlight.Dec()

			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			elapsed := time.Since(start).Seconds()
			routeLabel := route(r)

			m.requests.WithLabelValues(r.Method, routeLabel, strconv.Itoa(rec.status)).Inc()
			m.duration.WithLabelValues(r.Method, routeLabel).Observe(elapsed)

			// Only 5xx counts as a service error. A 4xx means the client sent
			// something wrong, and folding those in would make an error-rate
			// alert fire on user typos rather than on outages.
			if rec.status >= 500 {
				m.errors.WithLabelValues(r.Method, routeLabel).Inc()
			}
		})
	}
}

// RecordRateLimitDecision records one rate limiter decision.
//
// allowed vs denied is the pair that matters operationally: a rising denied
// rate is either an attack or a limit set too low, and the two are told apart
// by whether the allowed rate fell at the same time.
func (m *Metrics) RecordRateLimitDecision(algorithm string, allowed bool, remaining int) {
	decision := "denied"
	if allowed {
		decision = "allowed"
	}
	m.rateLimitDecisions.WithLabelValues(algorithm, decision).Inc()

	// Sampled rather than tracked exactly: this is the remaining allowance of
	// whichever key was most recently seen, which is a coarse health signal,
	// not a per-key gauge. A gauge per key would be unbounded cardinality.
	m.rateLimitRemaining.WithLabelValues(algorithm).Set(float64(remaining))
}

// RecordEventPublished counts a saga event leaving a service.
func (m *Metrics) RecordEventPublished(topic, eventType string) {
	m.eventsPublished.WithLabelValues(topic, eventType).Inc()
}

// RecordEventConsumed counts a saga event handled by a service.
//
// outcome distinguishes success from a failure that will be redelivered, which
// is the signal that a consumer is stuck in a retry loop — invisible from the
// published counts alone.
func (m *Metrics) RecordEventConsumed(topic, eventType, outcome string) {
	m.eventsConsumed.WithLabelValues(topic, eventType, outcome).Inc()
}

// RecordSagaOutcome counts an order reaching a terminal saga state.
func (m *Metrics) RecordSagaOutcome(outcome string) {
	m.sagaOutcomes.WithLabelValues(outcome).Inc()
}
