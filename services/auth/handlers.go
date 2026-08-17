package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"pkg/correlation"
	"pkg/httpx"
	"pkg/metrics"
)

// Password length bounds.
//
// The minimum follows current NIST SP 800-63B guidance, which favors length
// over composition rules. The maximum exists because bcrypt silently truncates
// input beyond 72 bytes: without an explicit limit, two different long
// passwords sharing a 72-byte prefix would be interchangeable, and a user who
// set a 100-character password would be misled about its strength.
const (
	minPasswordLen = 8
	maxPasswordLen = 72
)

// API holds the auth service's dependencies.
type API struct {
	store     *Store
	metrics   *metrics.Metrics
	logger    *slog.Logger
	jwtSecret []byte
	jwtTTL    time.Duration
	jwtIssuer string
	// bcryptCost is configurable so tests can drop to the minimum. At the
	// default cost a single hash takes ~100ms by design, which would make an
	// integration suite that creates many users unbearably slow.
	bcryptCost int
}

// Routes returns the service's HTTP handler.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(correlation.Middleware)

	// Metrics middleware runs inside correlation so a failure it records is
	// attributable, and after routing so ChiRoute can read the matched pattern
	// rather than the concrete path.
	if a.metrics != nil {
		r.Use(a.metrics.Middleware(metrics.ChiRoute))
		r.Handle("/metrics", a.metrics.Handler())
	}

	r.Get("/healthz", a.health)

	// Paths are prefixed with /auth because the gateway proxies /auth/* through
	// without stripping the prefix.
	r.Route("/auth", func(r chi.Router) {
		r.Post("/signup", a.signup)
		r.Post("/login", a.login)
		r.Get("/me", a.me)
	})
	return r
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type authResponse struct {
	Token     string   `json:"token"`
	ExpiresAt int64    `json:"expires_at"`
	User      userView `json:"user"`
}

type userView struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

// signup creates an account and returns a token, so a new user is logged in
// without a second round trip.
func (a *API) signup(w http.ResponseWriter, r *http.Request) {
	var req credentials
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	email, err := normalizeEmail(req.Email)
	if err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := validatePassword(req.Password); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), a.bcryptCost)
	if err != nil {
		a.logger.ErrorContext(r.Context(), "hashing password", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not create account")
		return
	}

	user, err := a.store.CreateUser(r.Context(), newUUID(), email, string(hash))
	if err != nil {
		if errors.Is(err, ErrEmailTaken) {
			// This does leak that the address is registered. Signup cannot
			// avoid it — the account either gets created or it does not, and
			// pretending otherwise would break the flow. Login, where the leak
			// would be far more useful to an attacker, does avoid it.
			httpx.WriteError(w, r, http.StatusConflict, "email already registered")
			return
		}
		a.logger.ErrorContext(r.Context(), "creating user", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not create account")
		return
	}

	a.logger.InfoContext(r.Context(), "user signed up", "user_id", user.ID)
	a.respondWithToken(w, r, user, http.StatusCreated)
}

// login exchanges credentials for a token.
func (a *API) login(w http.ResponseWriter, r *http.Request) {
	var req credentials
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	user, err := a.store.UserByEmail(r.Context(), req.Email)
	if err != nil && !errors.Is(err, ErrUserNotFound) {
		a.logger.ErrorContext(r.Context(), "looking up user", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not sign in")
		return
	}

	// Compare a hash even when no user was found, against a fixed dummy hash.
	// Returning early on a missing user would make "unknown email" measurably
	// faster than "wrong password", letting an attacker enumerate registered
	// addresses by timing alone.
	hash := dummyHash
	if user != nil {
		hash = []byte(user.PasswordHash)
	}
	compareErr := bcrypt.CompareHashAndPassword(hash, []byte(req.Password))

	if user == nil || compareErr != nil {
		// One message for both cases, so the response body does not reveal
		// which of the two failed.
		httpx.WriteError(w, r, http.StatusUnauthorized, "invalid email or password")
		return
	}

	a.logger.InfoContext(r.Context(), "user signed in", "user_id", user.ID)
	a.respondWithToken(w, r, user, http.StatusOK)
}

// me returns the caller's own account, identified by the header the gateway
// sets after validating the JWT.
func (a *API) me(w http.ResponseWriter, r *http.Request) {
	subject := httpx.Subject(r)
	if subject == "" {
		// Reached only if the service is called directly rather than through
		// the gateway's protected routes.
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthenticated")
		return
	}

	user, err := a.store.UserByID(r.Context(), subject)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "user not found")
			return
		}
		a.logger.ErrorContext(r.Context(), "looking up user", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not load account")
		return
	}

	httpx.WriteJSON(w, r, http.StatusOK, toView(user))
}

// respondWithToken issues a JWT and writes it with the user.
func (a *API) respondWithToken(w http.ResponseWriter, r *http.Request, user *User, status int) {
	expiresAt := time.Now().Add(a.jwtTTL)

	claims := jwt.RegisteredClaims{
		// Subject is what the gateway forwards downstream as X-User-ID, and
		// what the rate limiter keys authenticated traffic by.
		Subject:   user.ID,
		ExpiresAt: jwt.NewNumericDate(expiresAt),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		NotBefore: jwt.NewNumericDate(time.Now()),
		Issuer:    a.jwtIssuer,
		ID:        newUUID(),
	}

	// HS256 to match what the gateway validates (IMPLEMENTATION_PLAN.md 1.7).
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(a.jwtSecret)
	if err != nil {
		a.logger.ErrorContext(r.Context(), "signing token", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not issue token")
		return
	}

	httpx.WriteJSON(w, r, status, authResponse{
		Token:     signed,
		ExpiresAt: expiresAt.Unix(),
		User:      toView(user),
	})
}

func toView(u *User) userView {
	// PasswordHash is deliberately absent: a response type that cannot carry
	// the hash cannot leak it by accident.
	return userView{ID: u.ID, Email: u.Email, CreatedAt: u.CreatedAt}
}

// normalizeEmail validates and canonicalizes an address.
func normalizeEmail(raw string) (string, error) {
	e := strings.TrimSpace(raw)
	if e == "" {
		return "", errors.New("email is required")
	}
	if len(e) > 254 { // RFC 5321 maximum
		return "", errors.New("email is too long")
	}
	addr, err := mail.ParseAddress(e)
	if err != nil {
		return "", errors.New("email is not a valid address")
	}
	// ParseAddress accepts `Name <a@b.com>`; keep only the address itself.
	return addr.Address, nil
}

func validatePassword(p string) error {
	if len(p) < minPasswordLen {
		return errors.New("password must be at least 8 characters")
	}
	// Length in bytes, not runes: bcrypt's 72-byte limit is on the encoded
	// input, so a short string of multi-byte characters can still exceed it.
	if len(p) > maxPasswordLen {
		return errors.New("password must be at most 72 bytes")
	}
	return nil
}

// dummyHash is a valid bcrypt hash used for the timing-equalizing comparison in
// login. It is computed once at startup from a random password, so no real
// account can ever match it.
var dummyHash = func() []byte {
	var b [32]byte
	_, _ = rand.Read(b[:])
	h, err := bcrypt.GenerateFromPassword(b[:], bcrypt.MinCost)
	if err != nil {
		// Unreachable: bcrypt only fails on an invalid cost.
		panic("auth: generating dummy hash: " + err.Error())
	}
	return h
}()

// newUUID returns a random RFC 4122 version 4 UUID.
//
// Hand-rolled rather than pulled from a dependency: the service needs exactly
// this and nothing else from a UUID library.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
