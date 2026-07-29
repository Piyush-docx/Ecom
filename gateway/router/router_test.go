package router_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"gateway/middleware"
	"gateway/router"
	"ratelimiter"
	"ratelimiter/algorithms"
)

// testSecret is 32 bytes, the minimum JWTConfig.Validate accepts.
var testSecret = []byte("test-secret-at-least-32-bytes-ok")

// stubService stands in for a backend service. Phase 3's acceptance criteria
// say services may be stubs at this point; Phase 4 replaces them.
type stubService struct {
	*httptest.Server
	lastRequest chan *http.Request
}

func newStubService(t *testing.T) *stubService {
	t.Helper()

	s := &stubService{lastRequest: make(chan *http.Request, 100)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.lastRequest <- r.Clone(r.Context())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"stub":true}`))
	}))
	t.Cleanup(s.Close)
	return s
}

// received returns the most recent request the stub saw.
func (s *stubService) received(t *testing.T) *http.Request {
	t.Helper()
	select {
	case r := <-s.lastRequest:
		return r
	case <-time.After(time.Second):
		t.Fatal("stub service received no request")
		return nil
	}
}

// newTestGateway builds a gateway with an in-memory limiter and a manual clock.
//
// The in-memory limiter is used deliberately: these tests are about the
// gateway's HTTP behavior, and the Redis implementations already have their own
// integration tests. Both satisfy ratelimiter.Limiter, so the middleware cannot
// tell them apart.
func newTestGateway(t *testing.T, limit int, window time.Duration) (http.Handler, *stubService, *ratelimiter.ManualClock) {
	t.Helper()

	clk := ratelimiter.NewManualClock(time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC))
	limiter, err := algorithms.NewTokenBucket(limit, window, clk)
	if err != nil {
		t.Fatalf("constructing limiter: %v", err)
	}

	stub := newStubService(t)
	h, err := router.New(router.Config{
		Limiter: limiter,
		JWT:     middleware.JWTConfig{Secret: testSecret},
		Services: map[string]string{
			"auth":    stub.URL,
			"catalog": stub.URL,
			"orders":  stub.URL,
			"payment": stub.URL,
		},
	})
	if err != nil {
		t.Fatalf("building router: %v", err)
	}
	return h, stub, clk
}

// signToken returns a valid HS256 token for sub, expiring in an hour.
func signToken(t *testing.T, sub string) string {
	t.Helper()
	return signTokenWith(t, jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{
		Subject:   sub,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
	})
}

func signTokenWith(t *testing.T, method jwt.SigningMethod, key any, claims jwt.RegisteredClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(method, claims)
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("signing token: %v", err)
	}
	return s
}

// do issues a request against the gateway and returns the response.
func do(t *testing.T, h http.Handler, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = "192.0.2.10:1234"
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestRateLimitHeadersOnAllowedAndDeniedPaths is the Phase 3 acceptance
// criterion: an integration test hitting the gateway directly confirming the
// headers are present and correct on both the allowed and the denied path.
func TestRateLimitHeadersOnAllowedAndDeniedPaths(t *testing.T) {
	const limit = 3
	h, _, _ := newTestGateway(t, limit, time.Minute)
	token := signToken(t, "user-1")

	// Allowed path: every response carries the full X-RateLimit-* family, with
	// Remaining counting down. Headers on success are what let a well-behaved
	// client pace itself instead of discovering the limit by being refused.
	for i := 1; i <= limit; i++ {
		rec := do(t, h, http.MethodGet, "/catalog/products", token)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d", i, rec.Code, http.StatusOK)
		}
		if got := rec.Header().Get(middleware.RateLimitLimitHeader); got != strconv.Itoa(limit) {
			t.Errorf("request %d: %s = %q, want %q", i, middleware.RateLimitLimitHeader, got, strconv.Itoa(limit))
		}
		if want := strconv.Itoa(limit - i); rec.Header().Get(middleware.RateLimitRemainingHeader) != want {
			t.Errorf("request %d: %s = %q, want %q", i,
				middleware.RateLimitRemainingHeader, rec.Header().Get(middleware.RateLimitRemainingHeader), want)
		}
		if rec.Header().Get(middleware.RateLimitResetHeader) == "" {
			t.Errorf("request %d: %s is missing on an allowed response", i, middleware.RateLimitResetHeader)
		}
		// Retry-After is meaningful only on a denial; sending it on success
		// would tell a client to back off when it need not.
		if got := rec.Header().Get(middleware.RetryAfterHeader); got != "" {
			t.Errorf("request %d: %s = %q on an allowed response, want it absent", i, middleware.RetryAfterHeader, got)
		}
	}

	// Denied path: 429, the same headers still present, Remaining at 0, and a
	// Retry-After the client can act on.
	rec := do(t, h, http.MethodGet, "/catalog/products", token)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-limit request: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get(middleware.RateLimitLimitHeader); got != strconv.Itoa(limit) {
		t.Errorf("denied: %s = %q, want %q", middleware.RateLimitLimitHeader, got, strconv.Itoa(limit))
	}
	if got := rec.Header().Get(middleware.RateLimitRemainingHeader); got != "0" {
		t.Errorf("denied: %s = %q, want %q", middleware.RateLimitRemainingHeader, got, "0")
	}
	if got := rec.Header().Get(middleware.RateLimitResetHeader); got == "" {
		t.Errorf("denied: %s is missing", middleware.RateLimitResetHeader)
	}

	retryAfter := rec.Header().Get(middleware.RetryAfterHeader)
	if retryAfter == "" {
		t.Fatalf("denied: %s is missing", middleware.RetryAfterHeader)
	}
	// RFC 9110 allows only an integer number of seconds or an HTTP-date. A
	// fractional value such as "0.6" would be unparseable by clients.
	secs, err := strconv.Atoi(retryAfter)
	if err != nil {
		t.Errorf("denied: %s = %q, want an integer number of seconds", middleware.RetryAfterHeader, retryAfter)
	}
	// Zero would invite an immediate retry that is certain to fail again.
	if secs < 1 {
		t.Errorf("denied: %s = %d, want at least 1", middleware.RetryAfterHeader, secs)
	}

	// The error body should be JSON and name the reason.
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Errorf("denied: body is not JSON: %v (body=%q)", err, rec.Body.String())
	} else if body["error"] == "" {
		t.Error("denied: body has no error message")
	}
}

// TestRetryAfterNeverZeroForSubSecondWaits covers the case where the real wait
// is a fraction of a second.
//
// RFC 9110 permits only whole seconds, so a sub-second wait must round up to 1.
// Truncating instead would emit "Retry-After: 0", telling a client to retry
// immediately into a refusal that is certain — a hot loop against the gateway,
// which is the opposite of what the header is for.
//
// A high limit over a short window makes the per-token refill sub-second: 100
// tokens per second means one token every 10ms.
func TestRetryAfterNeverZeroForSubSecondWaits(t *testing.T) {
	const limit = 100
	h, _, _ := newTestGateway(t, limit, time.Second)
	token := signToken(t, "user-1")

	for i := 0; i < limit; i++ {
		if rec := do(t, h, http.MethodGet, "/catalog/products", token); rec.Code != http.StatusOK {
			t.Fatalf("setup request %d: status = %d, want 200", i+1, rec.Code)
		}
	}

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}

	got := rec.Header().Get(middleware.RetryAfterHeader)
	secs, err := strconv.Atoi(got)
	if err != nil {
		t.Fatalf("%s = %q, want an integer", middleware.RetryAfterHeader, got)
	}
	if secs < 1 {
		t.Errorf("%s = %d for a sub-second wait, want at least 1 — "+
			"a zero tells the client to retry immediately into a certain refusal",
			middleware.RetryAfterHeader, secs)
	}
}

// TestRateLimitRecoversAfterWindow confirms a throttled client is admitted
// again once the limiter refills, and that the headers reflect it.
func TestRateLimitRecoversAfterWindow(t *testing.T) {
	const (
		limit  = 2
		window = time.Minute
	)
	h, _, clk := newTestGateway(t, limit, window)
	token := signToken(t, "user-1")

	for i := 0; i < limit; i++ {
		if rec := do(t, h, http.MethodGet, "/catalog/products", token); rec.Code != http.StatusOK {
			t.Fatalf("setup request %d: status = %d, want 200", i+1, rec.Code)
		}
	}
	if rec := do(t, h, http.MethodGet, "/catalog/products", token); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected the client to be throttled, got status %d", rec.Code)
	}

	clk.Advance(window)

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusOK {
		t.Errorf("after the window elapsed: status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(middleware.RateLimitRemainingHeader); got != strconv.Itoa(limit-1) {
		t.Errorf("after the window elapsed: %s = %q, want %q",
			middleware.RateLimitRemainingHeader, got, strconv.Itoa(limit-1))
	}
}

// TestRateLimitIsPerUser confirms one user's exhaustion does not throttle
// another. Keying by JWT subject rather than IP is what makes this hold for
// users sharing an address behind NAT.
func TestRateLimitIsPerUser(t *testing.T) {
	const limit = 2
	h, _, _ := newTestGateway(t, limit, time.Minute)

	alice := signToken(t, "alice")
	bob := signToken(t, "bob")

	for i := 0; i < limit; i++ {
		if rec := do(t, h, http.MethodGet, "/catalog/products", alice); rec.Code != http.StatusOK {
			t.Fatalf("alice request %d: status = %d, want 200", i+1, rec.Code)
		}
	}
	if rec := do(t, h, http.MethodGet, "/catalog/products", alice); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("alice over limit: status = %d, want 429", rec.Code)
	}

	// Bob shares alice's IP in this test but has his own budget.
	for i := 0; i < limit; i++ {
		if rec := do(t, h, http.MethodGet, "/catalog/products", bob); rec.Code != http.StatusOK {
			t.Errorf("bob request %d: status = %d, want 200 — the limit is not keyed per user", i+1, rec.Code)
		}
	}
}

// TestPublicRoutesNeedNoToken confirms the auth routes are reachable without a
// JWT. Login and signup cannot require the token they exist to issue.
func TestPublicRoutesNeedNoToken(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	rec := do(t, h, http.MethodPost, "/auth/login", "")
	if rec.Code != http.StatusOK {
		t.Errorf("POST /auth/login without a token: status = %d, want 200", rec.Code)
	}
	// Still rate limited, keyed by IP rather than user.
	if rec.Header().Get(middleware.RateLimitLimitHeader) == "" {
		t.Errorf("public route is missing %s — public traffic must still be limited",
			middleware.RateLimitLimitHeader)
	}
}

// TestProtectedRoutesRequireToken confirms protected routes reject anonymous
// requests with 401 and a WWW-Authenticate challenge, as RFC 9235 requires.
func TestProtectedRoutesRequireToken(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	for _, path := range []string{"/catalog/products", "/orders/123", "/payment/charge"} {
		rec := do(t, h, http.MethodGet, path, "")
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without a token: status = %d, want %d", path, rec.Code, http.StatusUnauthorized)
		}
		if got := rec.Header().Get("WWW-Authenticate"); got == "" {
			t.Errorf("GET %s: 401 response has no WWW-Authenticate header", path)
		}
	}
}

// TestCorrelationIDIsAlwaysPresent confirms every response carries a
// correlation ID, including rejections. Phase 6 depends on this: a request that
// was refused is exactly the one a user will ask about.
func TestCorrelationIDIsAlwaysPresent(t *testing.T) {
	const limit = 1
	h, _, _ := newTestGateway(t, limit, time.Minute)
	token := signToken(t, "user-1")

	cases := []struct {
		name   string
		path   string
		token  string
		status int
	}{
		{"allowed", "/catalog/products", token, http.StatusOK},
		{"rate limited", "/catalog/products", token, http.StatusTooManyRequests},
		{"unauthorized", "/orders/1", "", http.StatusUnauthorized},
		{"health", "/healthz", "", http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, tc.path, tc.token)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if id := rec.Header().Get(middleware.CorrelationIDHeader); id == "" {
				t.Errorf("%s response has no %s header", tc.name, middleware.CorrelationIDHeader)
			}
		})
	}
}

// TestCorrelationIDIsPropagatedDownstream confirms an inbound ID reaches the
// upstream service unchanged, so one ID spans the whole request lifecycle.
func TestCorrelationIDIsPropagatedDownstream(t *testing.T) {
	h, stub, _ := newTestGateway(t, 10, time.Minute)
	token := signToken(t, "user-1")

	const id = "client-supplied-trace-id"
	req := httptest.NewRequest(http.MethodGet, "/catalog/products", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(middleware.CorrelationIDHeader, id)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(middleware.CorrelationIDHeader); got != id {
		t.Errorf("response %s = %q, want the inbound %q", middleware.CorrelationIDHeader, got, id)
	}
	if got := stub.received(t).Header.Get(middleware.CorrelationIDHeader); got != id {
		t.Errorf("upstream saw %s = %q, want %q", middleware.CorrelationIDHeader, got, id)
	}
}

// TestSubjectForwardedDownstream confirms the gateway tells services who the
// user is, so they need not re-validate the token (IMPLEMENTATION_PLAN.md 1.7).
func TestSubjectForwardedDownstream(t *testing.T) {
	h, stub, _ := newTestGateway(t, 10, time.Minute)

	do(t, h, http.MethodGet, "/catalog/products", signToken(t, "user-42"))

	if got := stub.received(t).Header.Get(middleware.SubjectHeader); got != "user-42" {
		t.Errorf("upstream saw %s = %q, want %q", middleware.SubjectHeader, got, "user-42")
	}
}

// TestHealthzBypassesRateLimit confirms health checks are neither throttled nor
// authenticated.
//
// If health checks consumed quota, a load balancer polling every second would
// eventually exhaust it and every instance would be pulled from rotation — a
// self-inflicted outage.
func TestHealthzBypassesRateLimit(t *testing.T) {
	h, _, _ := newTestGateway(t, 1, time.Minute)

	for i := 0; i < 20; i++ {
		rec := do(t, h, http.MethodGet, "/healthz", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("health check %d: status = %d, want 200 — health must not be rate limited", i+1, rec.Code)
		}
	}
}

// TestRouterRejectsInvalidConfig confirms misconfiguration fails at startup
// rather than at the first request.
func TestRouterRejectsInvalidConfig(t *testing.T) {
	limiter, err := algorithms.NewTokenBucket(10, time.Minute, nil)
	if err != nil {
		t.Fatalf("constructing limiter: %v", err)
	}
	valid := middleware.JWTConfig{Secret: testSecret}

	cases := []struct {
		name string
		cfg  router.Config
	}{
		{"no limiter", router.Config{JWT: valid}},
		{"no jwt secret", router.Config{Limiter: limiter}},
		{"short jwt secret", router.Config{Limiter: limiter, JWT: middleware.JWTConfig{Secret: []byte("too-short")}}},
		{"relative service url", router.Config{
			Limiter:  limiter,
			JWT:      valid,
			Services: map[string]string{"auth": "/not-absolute"},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := router.New(tc.cfg); err == nil {
				t.Error("New returned no error, want one")
			}
		})
	}
}
