package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"pkg/correlation"
	"pkg/dbx"
	"pkg/httpx"
)

// These are integration tests against a real Postgres, as
// IMPLEMENTATION_PLAN.md Phase 4 requires: "not mocks for the DB layer". A
// mocked database would prove nothing about the unique index that enforces
// email uniqueness, which is the only thing standing between two concurrent
// signups and a duplicate account.
//
// Start one with:
//
//	docker compose -f deploy/docker-compose.yml up -d postgres
//
// Override with AUTH_TEST_DATABASE_URL.

const testJWTSecret = "test-secret-at-least-32-bytes-long"

func testDSN() string {
	if v := os.Getenv("AUTH_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://ecom:ecom@localhost:5432/ecom_auth?sslmode=disable"
}

// newTestAPI connects to Postgres, applies migrations, and returns a ready API.
//
// It skips rather than fails when no database is reachable, so `go test ./...`
// on a machine without docker reports honestly instead of implying the DB layer
// was exercised.
func newTestAPI(t *testing.T) (*API, *pgxpool.Pool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := dbx.Connect(ctx, testDSN())
	if err != nil {
		t.Skipf("no Postgres at %s (%v) — start one with: docker compose -f deploy/docker-compose.yml up -d postgres", testDSN(), err)
	}
	t.Cleanup(pool.Close)

	if err := dbx.Migrate(testDSN(), migrationsFS, "migrations"); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}

	return &API{
		store:  NewStore(pool),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		// The tests do not assert on log output, so discard it rather than
		// flooding the test log.
		jwtSecret: []byte(testJWTSecret),
		jwtTTL:    time.Hour,
		jwtIssuer: "test",
		// MinCost keeps the suite fast. The default cost takes ~100ms per hash
		// by design, which would dominate the runtime of a suite that creates
		// dozens of users.
		bcryptCost: bcrypt.MinCost,
	}, pool
}

// uniqueEmail returns an address unique to one test run, so tests neither
// collide with each other nor require truncating a shared table.
func uniqueEmail(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d@example.test",
		strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano())
}

// doJSON issues a request against the API and returns the response recorder.
func doJSON(t *testing.T, api *API, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encoding request body: %v", err)
		}
	}

	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)
	return rec
}

func decodeAuth(t *testing.T, rec *httptest.ResponseRecorder) authResponse {
	t.Helper()
	var got authResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding auth response: %v (body=%q)", err, rec.Body.String())
	}
	return got
}

// TestSignupAndLogin covers the primary flow end to end against real Postgres:
// an account is created, a token is issued, and the same credentials work on a
// subsequent login.
func TestSignupAndLogin(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	const password = "correct-horse-battery"

	rec := doJSON(t, api, http.MethodPost, "/auth/signup", credentials{Email: email, Password: password}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("signup: status = %d, want %d (body=%s)", rec.Code, http.StatusCreated, rec.Body)
	}

	signed := decodeAuth(t, rec)
	if signed.Token == "" {
		t.Error("signup returned no token")
	}
	if signed.User.ID == "" {
		t.Error("signup returned no user id")
	}
	if signed.User.Email != email {
		t.Errorf("signup returned email %q, want %q", signed.User.Email, email)
	}
	if signed.ExpiresAt <= time.Now().Unix() {
		t.Errorf("signup token expires_at = %d, want a future timestamp", signed.ExpiresAt)
	}

	// The token must be exactly what the gateway expects: HS256, subject set to
	// the user ID. If this drifts, the gateway rejects every token this service
	// issues and the whole system is broken.
	claims := &jwt.RegisteredClaims{}
	parsed, err := jwt.ParseWithClaims(signed.Token, claims, func(*jwt.Token) (any, error) {
		return []byte(testJWTSecret), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil {
		t.Fatalf("gateway would reject the issued token: %v", err)
	}
	if !parsed.Valid {
		t.Error("issued token is not valid")
	}
	if claims.Subject != signed.User.ID {
		t.Errorf("token subject = %q, want the user id %q", claims.Subject, signed.User.ID)
	}

	// Logging in with the same credentials returns a fresh token.
	rec = doJSON(t, api, http.MethodPost, "/auth/login", credentials{Email: email, Password: password}, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("login: status = %d, want %d (body=%s)", rec.Code, http.StatusOK, rec.Body)
	}
	loggedIn := decodeAuth(t, rec)
	if loggedIn.User.ID != signed.User.ID {
		t.Errorf("login returned user id %q, want %q", loggedIn.User.ID, signed.User.ID)
	}
}

// TestPasswordIsHashed confirms the plaintext password never reaches the
// database. This reads the row directly rather than going through the store,
// because the point is what is physically stored.
func TestPasswordIsHashed(t *testing.T) {
	api, pool := newTestAPI(t)

	email := uniqueEmail(t)
	const password = "correct-horse-battery"

	rec := doJSON(t, api, http.MethodPost, "/auth/signup", credentials{Email: email, Password: password}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("signup: status = %d (body=%s)", rec.Code, rec.Body)
	}

	var stored string
	err := pool.QueryRow(context.Background(),
		`SELECT password_hash FROM users WHERE lower(email) = lower($1)`, email).Scan(&stored)
	if err != nil {
		t.Fatalf("reading stored hash: %v", err)
	}

	if stored == password {
		t.Fatal("the password is stored in plaintext")
	}
	if !strings.HasPrefix(stored, "$2") {
		t.Errorf("stored value %q is not a bcrypt hash", stored)
	}
	// The hash must actually verify, or nobody could ever log in.
	if err := bcrypt.CompareHashAndPassword([]byte(stored), []byte(password)); err != nil {
		t.Errorf("stored hash does not verify against the password: %v", err)
	}
}

// TestPasswordHashNeverLeavesTheService confirms no response body carries the
// hash. Leaking it would let an attacker crack it offline at leisure.
func TestPasswordHashNeverLeavesTheService(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	const password = "correct-horse-battery"

	signupRec := doJSON(t, api, http.MethodPost, "/auth/signup", credentials{Email: email, Password: password}, nil)
	loginRec := doJSON(t, api, http.MethodPost, "/auth/login", credentials{Email: email, Password: password}, nil)
	meRec := doJSON(t, api, http.MethodGet, "/auth/me", nil,
		map[string]string{httpx.SubjectHeader: decodeAuth(t, signupRec).User.ID})

	for name, rec := range map[string]*httptest.ResponseRecorder{
		"signup": signupRec, "login": loginRec, "me": meRec,
	} {
		body := rec.Body.String()
		if strings.Contains(body, password) {
			t.Errorf("%s response contains the plaintext password", name)
		}
		if strings.Contains(body, "$2a$") || strings.Contains(body, "$2b$") {
			t.Errorf("%s response contains a bcrypt hash: %s", name, body)
		}
		if strings.Contains(strings.ToLower(body), "password_hash") {
			t.Errorf("%s response mentions password_hash: %s", name, body)
		}
	}
}

// TestDuplicateEmailIsRejected confirms the unique index is enforced.
func TestDuplicateEmailIsRejected(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	body := credentials{Email: email, Password: "correct-horse-battery"}

	if rec := doJSON(t, api, http.MethodPost, "/auth/signup", body, nil); rec.Code != http.StatusCreated {
		t.Fatalf("first signup: status = %d (body=%s)", rec.Code, rec.Body)
	}

	rec := doJSON(t, api, http.MethodPost, "/auth/signup", body, nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("duplicate signup: status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

// TestDuplicateEmailIsRejectedUnderConcurrency is the reason the DB layer is
// not mocked.
//
// Several goroutines sign up with the same address simultaneously. Exactly one
// must succeed.
//
// Verified by experiment rather than assumed: with the unique index dropped,
// all 10 concurrent signups succeed and create 10 duplicate accounts. The index
// is the only thing preventing that — an application-level check cannot, since
// every goroutine observes "no such user" before any of them inserts. This test
// therefore fails loudly if the index is ever removed from the migration, which
// is precisely why the DB layer is not mocked here.
func TestDuplicateEmailIsRejectedUnderConcurrency(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	const attempts = 10

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		created   int
		conflicts int
		other     []int
	)

	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := doJSON(t, api, http.MethodPost, "/auth/signup",
				credentials{Email: email, Password: "correct-horse-battery"}, nil)

			mu.Lock()
			defer mu.Unlock()
			switch rec.Code {
			case http.StatusCreated:
				created++
			case http.StatusConflict:
				conflicts++
			default:
				other = append(other, rec.Code)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(other) > 0 {
		t.Errorf("unexpected statuses %v, want only 201 and 409", other)
	}
	if created != 1 {
		t.Errorf("%d concurrent signups for one address created %d accounts, want exactly 1", attempts, created)
	}
	if conflicts != attempts-1 {
		t.Errorf("got %d conflicts, want %d", conflicts, attempts-1)
	}
}

// TestEmailIsCaseInsensitive confirms Alice@x.test and alice@x.test are the
// same account. Treating them as distinct invites account-confusion attacks.
func TestEmailIsCaseInsensitive(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	const password = "correct-horse-battery"

	if rec := doJSON(t, api, http.MethodPost, "/auth/signup",
		credentials{Email: email, Password: password}, nil); rec.Code != http.StatusCreated {
		t.Fatalf("signup: status = %d (body=%s)", rec.Code, rec.Body)
	}

	upper := strings.ToUpper(email)

	if rec := doJSON(t, api, http.MethodPost, "/auth/signup",
		credentials{Email: upper, Password: password}, nil); rec.Code != http.StatusConflict {
		t.Errorf("signup with a differently-cased address: status = %d, want %d (duplicate)",
			rec.Code, http.StatusConflict)
	}

	if rec := doJSON(t, api, http.MethodPost, "/auth/login",
		credentials{Email: upper, Password: password}, nil); rec.Code != http.StatusOK {
		t.Errorf("login with a differently-cased address: status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestLoginFailuresAreIndistinguishable confirms an unknown email and a wrong
// password produce identical responses. Any difference lets an attacker
// enumerate which addresses are registered.
func TestLoginFailuresAreIndistinguishable(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	if rec := doJSON(t, api, http.MethodPost, "/auth/signup",
		credentials{Email: email, Password: "correct-horse-battery"}, nil); rec.Code != http.StatusCreated {
		t.Fatalf("signup: status = %d", rec.Code)
	}

	wrongPassword := doJSON(t, api, http.MethodPost, "/auth/login",
		credentials{Email: email, Password: "wrong-password-entirely"}, nil)
	unknownEmail := doJSON(t, api, http.MethodPost, "/auth/login",
		credentials{Email: uniqueEmail(t), Password: "correct-horse-battery"}, nil)

	if wrongPassword.Code != http.StatusUnauthorized {
		t.Errorf("wrong password: status = %d, want 401", wrongPassword.Code)
	}
	if unknownEmail.Code != http.StatusUnauthorized {
		t.Errorf("unknown email: status = %d, want 401", unknownEmail.Code)
	}

	var a, b httpx.ErrorResponse
	_ = json.Unmarshal(wrongPassword.Body.Bytes(), &a)
	_ = json.Unmarshal(unknownEmail.Body.Bytes(), &b)

	if a.Error != b.Error {
		t.Errorf("wrong password says %q but unknown email says %q — "+
			"the difference lets an attacker enumerate registered addresses", a.Error, b.Error)
	}
}

// TestSignupValidation covers rejected inputs.
func TestSignupValidation(t *testing.T) {
	api, _ := newTestAPI(t)

	cases := []struct {
		name  string
		email string
		pass  string
	}{
		{"empty email", "", "correct-horse-battery"},
		{"malformed email", "not-an-email", "correct-horse-battery"},
		{"empty password", "valid@example.test", ""},
		{"short password", "valid@example.test", "short"},
		{"password over bcrypt's 72-byte limit", "valid@example.test", strings.Repeat("a", 73)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doJSON(t, api, http.MethodPost, "/auth/signup",
				credentials{Email: tc.email, Password: tc.pass}, nil)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d (body=%s)", rec.Code, http.StatusBadRequest, rec.Body)
			}
		})
	}
}

// TestMeRequiresGatewayHeader confirms /auth/me identifies the caller from the
// header the gateway sets, and refuses the request without it.
func TestMeRequiresGatewayHeader(t *testing.T) {
	api, _ := newTestAPI(t)

	email := uniqueEmail(t)
	rec := doJSON(t, api, http.MethodPost, "/auth/signup",
		credentials{Email: email, Password: "correct-horse-battery"}, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("signup: status = %d", rec.Code)
	}
	userID := decodeAuth(t, rec).User.ID

	t.Run("with the gateway header", func(t *testing.T) {
		rec := doJSON(t, api, http.MethodGet, "/auth/me", nil,
			map[string]string{httpx.SubjectHeader: userID})
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body)
		}
		var got userView
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if got.ID != userID {
			t.Errorf("id = %q, want %q", got.ID, userID)
		}
	})

	t.Run("without the header", func(t *testing.T) {
		rec := doJSON(t, api, http.MethodGet, "/auth/me", nil, nil)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("with an unknown user id", func(t *testing.T) {
		rec := doJSON(t, api, http.MethodGet, "/auth/me", nil,
			map[string]string{httpx.SubjectHeader: "00000000-0000-4000-8000-000000000000"})
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

// TestCorrelationIDIsEchoed confirms the service participates in the tracing
// chain Phase 6 depends on.
func TestCorrelationIDIsEchoed(t *testing.T) {
	api, _ := newTestAPI(t)

	const id = "test-correlation-id-1234"
	rec := doJSON(t, api, http.MethodGet, "/healthz", nil,
		map[string]string{correlation.Header: id})

	if got := rec.Header().Get(correlation.Header); got != id {
		t.Errorf("%s = %q, want the inbound %q", correlation.Header, got, id)
	}
}

// TestMalformedRequestBodies covers the decoding paths.
func TestMalformedRequestBodies(t *testing.T) {
	api, _ := newTestAPI(t)

	cases := []struct {
		name string
		body string
	}{
		{"not json", `not json at all`},
		{"empty body", ``},
		{"unknown field", `{"email":"a@b.test","password":"correct-horse-battery","admin":true}`},
		{"two objects", `{"email":"a@b.test","password":"correct-horse-battery"}{"email":"c@d.test"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/auth/signup", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			api.Routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d (body=%s)", rec.Code, http.StatusBadRequest, rec.Body)
			}
		})
	}
}
