package router_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"gateway/middleware"
)

// These are the JWT attack cases AGENTS.md 3 calls for when it pairs Phase 3
// with security-and-hardening. Each asserts a rejection, so a regression that
// weakens validation shows up as a test failure rather than as a quiet
// authentication bypass.

// TestAlgNoneIsRejected covers the most serious JWT flaw: a token declaring
// "alg":"none" with an empty signature.
//
// A parser that trusts the token's own header would accept it as validly
// signed, letting anyone mint a token for any user.
//
// Note on what this test does and does not prove. golang-jwt/v5 refuses
// SigningMethodNone unless the keyfunc explicitly returns the
// jwt.UnsafeAllowNoneSignatureType sentinel, so this request is rejected by the
// library itself even with jwt.WithValidMethods removed — verified by
// mutation. The test therefore guards against a future change to the keyfunc
// that returns that sentinel, not against the loss of WithValidMethods.
// TestAlgConfusionIsRejected is what covers WithValidMethods, and that one does
// fail when the option is dropped.
func TestAlgNoneIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	// jwt.SigningMethodNone requires an explicit unsafe sentinel key, so the
	// token is hand-assembled the way an attacker would.
	header := base64URL(t, `{"alg":"none","typ":"JWT"}`)
	claims := base64URL(t, `{"sub":"attacker","exp":`+futureUnix()+`}`)
	token := header + "." + claims + "." // empty signature

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("alg=none token: status = %d, want %d — the gateway accepted an unsigned token",
			rec.Code, http.StatusUnauthorized)
	}
}

// TestAlgConfusionIsRejected covers the algorithm-confusion attack: a token
// signed with HMAC using a known RSA *public* key as the secret.
//
// Against a verifier that dispatches on the token's declared algorithm, a
// service that normally verifies RS256 would treat the public key as an HMAC
// secret and accept the forgery. Pinning to HS256 alone means a token
// declaring anything else never reaches signature verification.
//
// This gateway is HS256-only by design (IMPLEMENTATION_PLAN.md 1.7), so the
// test covers the general shape: a token signed with a different algorithm
// must be rejected on the algorithm check.
func TestAlgConfusionIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	// HS512 rather than HS256, signed with the correct secret. The signature
	// is cryptographically valid; only the algorithm differs. Accepting it
	// would prove the gateway dispatches on the attacker-controlled header.
	token := signTokenWith(t, jwt.SigningMethodHS512, testSecret, jwt.RegisteredClaims{
		Subject:   "attacker",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("HS512 token against an HS256-only gateway: status = %d, want %d — "+
			"the accepted algorithm is not pinned", rec.Code, http.StatusUnauthorized)
	}
}

// TestExpiredTokenIsRejected confirms exp is enforced, so a leaked token stops
// working.
func TestExpiredTokenIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	token := signTokenWith(t, jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{
		Subject:   "user-1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
	})

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expired token: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestTokenWithoutExpiryIsRejected confirms a token with no exp claim is
// refused. Without WithExpirationRequired such a token would be valid forever,
// so a single leak would be permanent.
func TestTokenWithoutExpiryIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	token := signTokenWith(t, jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{
		Subject: "user-1",
		// No ExpiresAt.
	})

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("token without exp: status = %d, want %d — such a token never expires",
			rec.Code, http.StatusUnauthorized)
	}
}

// TestWrongSignatureIsRejected confirms a token signed with a different secret
// fails, which is the base guarantee the whole scheme rests on.
func TestWrongSignatureIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	token := signTokenWith(t, jwt.SigningMethodHS256, []byte("a-different-secret-32-bytes-long"), jwt.RegisteredClaims{
		Subject:   "attacker",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("token signed with the wrong secret: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestTamperedPayloadIsRejected confirms editing the claims of an otherwise
// valid token invalidates it — a privilege-escalation attempt by swapping the
// subject.
func TestTamperedPayloadIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	valid := signToken(t, "user-1")
	parts := strings.Split(valid, ".")
	if len(parts) != 3 {
		t.Fatalf("expected a three-part token, got %d parts", len(parts))
	}
	// Swap the payload for one naming a different subject, keeping the original
	// signature.
	parts[1] = base64URL(t, `{"sub":"admin","exp":`+futureUnix()+`}`)
	tampered := strings.Join(parts, ".")

	rec := do(t, h, http.MethodGet, "/catalog/products", tampered)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("tampered payload: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestTokenWithoutSubjectIsRejected confirms a token that validates but
// identifies nobody is refused. Such a token would make the rate limiter key
// every anonymous-but-valid request together and give services an empty user.
func TestTokenWithoutSubjectIsRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	token := signTokenWith(t, jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		// No Subject.
	})

	rec := do(t, h, http.MethodGet, "/catalog/products", token)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("token with no subject: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestMalformedAuthorizationHeadersAreRejected covers the header parsing paths.
func TestMalformedAuthorizationHeadersAreRejected(t *testing.T) {
	h, _, _ := newTestGateway(t, 100, time.Minute)
	valid := signToken(t, "user-1")

	cases := []struct {
		name   string
		header string
	}{
		{"empty", ""},
		{"bearer with no token", "Bearer"},
		{"bearer with only spaces", "Bearer    "},
		{"wrong scheme", "Basic " + valid},
		{"no scheme", valid},
		{"garbage", "Bearer not-a-jwt"},
		{"two segments", "Bearer aaa.bbb"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/catalog/products", nil)
			req.RemoteAddr = "192.0.2.10:1234"
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("Authorization=%q: status = %d, want %d", tc.header, rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

// TestBearerSchemeIsCaseInsensitive confirms a correctly-formed token is
// accepted regardless of scheme casing, since RFC 7235 defines the auth scheme
// as case-insensitive. A client using "bearer" is not an attacker.
func TestBearerSchemeIsCaseInsensitive(t *testing.T) {
	h, _, _ := newTestGateway(t, 100, time.Minute)
	token := signToken(t, "user-1")

	for _, scheme := range []string{"Bearer", "bearer", "BEARER", "BeArEr"} {
		t.Run(scheme, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/catalog/products", nil)
			req.RemoteAddr = "192.0.2.10:1234"
			req.Header.Set("Authorization", scheme+" "+token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Errorf("scheme %q: status = %d, want 200", scheme, rec.Code)
			}
		})
	}
}

// TestClientCannotSpoofSubjectHeader is the counterpart to the trusted-header
// design. Services treat X-User-ID as proof of identity, so the gateway must
// guarantee a client cannot set it.
func TestClientCannotSpoofSubjectHeader(t *testing.T) {
	h, stub, _ := newTestGateway(t, 100, time.Minute)

	t.Run("on a protected route the JWT subject wins", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/catalog/products", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set("Authorization", "Bearer "+signToken(t, "real-user"))
		req.Header.Set(middleware.SubjectHeader, "admin")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := stub.received(t).Header.Get(middleware.SubjectHeader); got != "real-user" {
			t.Errorf("upstream saw %s = %q, want %q — a client spoofed its identity",
				middleware.SubjectHeader, got, "real-user")
		}
	})

	t.Run("on a public route the header is stripped", func(t *testing.T) {
		// This is the dangerous case: no JWT middleware runs on public routes,
		// so nothing would overwrite a spoofed header unless it is stripped
		// unconditionally at the edge.
		req := httptest.NewRequest(http.MethodPost, "/auth/login", nil)
		req.RemoteAddr = "192.0.2.10:1234"
		req.Header.Set(middleware.SubjectHeader, "admin")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := stub.received(t).Header.Get(middleware.SubjectHeader); got != "" {
			t.Errorf("upstream saw %s = %q on a public route, want it empty — "+
				"a client asserted an identity the gateway never verified",
				middleware.SubjectHeader, got)
		}
	})
}

// TestErrorResponsesDoNotLeakDetail confirms rejection messages stay generic.
// Telling an attacker whether a token was expired, badly signed, or malformed
// hands them a free oracle for probing.
func TestErrorResponsesDoNotLeakDetail(t *testing.T) {
	h, _, _ := newTestGateway(t, 10, time.Minute)

	expired := signTokenWith(t, jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{
		Subject:   "user-1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
	})
	wrongKey := signTokenWith(t, jwt.SigningMethodHS256, []byte("a-different-secret-32-bytes-long"), jwt.RegisteredClaims{
		Subject:   "user-1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})

	var messages []string
	for _, token := range []string{expired, wrongKey} {
		rec := do(t, h, http.MethodGet, "/catalog/products", token)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}

		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body is not JSON: %v", err)
		}
		msg, _ := body["error"].(string)
		messages = append(messages, msg)

		lower := strings.ToLower(msg)
		for _, leak := range []string{"expired", "signature", "malformed", "secret", "hmac"} {
			if strings.Contains(lower, leak) {
				t.Errorf("error message %q leaks the reason (%q)", msg, leak)
			}
		}
	}

	// Both failures must be indistinguishable to the client.
	if messages[0] != messages[1] {
		t.Errorf("expired and wrongly-signed tokens produced different messages (%q vs %q) — "+
			"the difference is an oracle for an attacker", messages[0], messages[1])
	}
}

// TestOversizedCorrelationIDIsReplaced confirms a caller-supplied correlation
// ID is validated rather than echoed blindly. An unbounded value is a cost on
// every log line, and control characters would let a caller forge log entries
// or split the response header.
func TestOversizedCorrelationIDIsReplaced(t *testing.T) {
	h, _, _ := newTestGateway(t, 100, time.Minute)

	cases := []struct {
		name string
		id   string
	}{
		{"too long", strings.Repeat("a", 200)},
		{"newline injection", "abc\ndef"},
		{"carriage return", "abc\r\nX-Injected: evil"},
		{"tab", "abc\tdef"},
		{"null byte", "abc\x00def"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			req.RemoteAddr = "192.0.2.10:1234"
			// Set directly on the map: Header.Set would sanitize some of these
			// before the middleware ever sees them.
			req.Header[middleware.CorrelationIDHeader] = []string{tc.id}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(middleware.CorrelationIDHeader)
			if got == tc.id {
				t.Errorf("%s: the gateway echoed the unsafe correlation ID verbatim", tc.name)
			}
			if got == "" {
				t.Errorf("%s: no correlation ID was assigned", tc.name)
			}
		})
	}
}

// base64URL encodes a JWT segment the way the spec requires: base64url with no
// padding.
func base64URL(t *testing.T, s string) string {
	t.Helper()
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// futureUnix returns an exp claim value an hour from now.
func futureUnix() string {
	return strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
}
