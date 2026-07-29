package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// SubjectHeader carries the authenticated user's ID to downstream services.
//
// IMPLEMENTATION_PLAN.md 1.7 specifies the JWT is "validated once at the
// gateway and passed downstream as a trusted header". That trust is only sound
// if services are unreachable except through the gateway — otherwise anyone
// could set this header directly and impersonate any user. Phase 7's compose
// topology must keep the service ports off the host network for this to hold.
const SubjectHeader = "X-User-ID"

// AuthorizationHeader is the standard header carrying the bearer token.
const AuthorizationHeader = "Authorization"

// Claims is the gateway's view of a token's payload. Services needing richer
// claims can parse the token themselves; the gateway only needs identity.
type Claims struct {
	jwt.RegisteredClaims
}

// ClaimsFromContext returns the validated claims carried by ctx. ok is false
// for a request that did not pass through RequireJWT — an unauthenticated
// request on a public route, for instance.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*Claims)
	return c, ok
}

// JWTConfig configures RequireJWT.
type JWTConfig struct {
	// Secret is the HS256 shared secret. It must match the auth service's
	// signing key.
	Secret []byte

	// Issuer, when non-empty, requires a matching iss claim.
	Issuer string

	// Audience, when non-empty, requires a matching aud claim.
	Audience string
}

// Validate reports whether the configuration is usable.
func (c JWTConfig) Validate() error {
	if len(c.Secret) == 0 {
		return errors.New("jwt: secret must not be empty")
	}
	// HS256 security rests entirely on secret entropy, since the signing key is
	// also the verification key. A short secret is brute-forceable offline from
	// a single captured token.
	if len(c.Secret) < 32 {
		return fmt.Errorf("jwt: secret must be at least 32 bytes, got %d", len(c.Secret))
	}
	return nil
}

// parserOptions builds the validation rules applied to every token.
func (c JWTConfig) parserOptions() []jwt.ParserOption {
	opts := []jwt.ParserOption{
		// The single most important option here. Without it, a token whose
		// header claims "alg":"none" would parse as valid with no signature at
		// all, and a token signed with an asymmetric algorithm could be
		// verified against our HMAC secret as if it were the public key —
		// the classic algorithm-confusion attack. Pinning the accepted method
		// to exactly HS256 closes both.
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),

		// A token with no exp claim would be valid forever. Requiring it means
		// a leaked token has a bounded lifetime.
		jwt.WithExpirationRequired(),
	}
	if c.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(c.Issuer))
	}
	if c.Audience != "" {
		opts = append(opts, jwt.WithAudience(c.Audience))
	}
	return opts
}

// RequireJWT validates a bearer token and rejects the request if it is missing
// or invalid. It is applied only to protected routes; login and signup cannot
// require a token they exist to issue.
//
// On success the claims are placed on the request context and the subject is
// forwarded downstream in SubjectHeader.
func RequireJWT(cfg JWTConfig) func(http.Handler) http.Handler {
	opts := cfg.parserOptions()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, err := bearerToken(r)
			if err != nil {
				unauthorized(w, r, "missing or malformed Authorization header")
				return
			}

			claims := &Claims{}
			_, err = jwt.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) {
				return cfg.Secret, nil
			}, opts...)
			if err != nil {
				// The reason is deliberately not echoed to the client: telling
				// an attacker whether a token was expired versus badly signed
				// is free information. The gateway's logs keep the detail.
				unauthorized(w, r, "invalid token")
				return
			}

			// A token that validates but identifies nobody is unusable: the
			// rate limiter would key every such request together, and services
			// would receive an empty user ID.
			if claims.Subject == "" {
				unauthorized(w, r, "token has no subject")
				return
			}

			// Overwrite rather than append. A client that sent its own
			// X-User-ID must not be able to smuggle it past the gateway.
			r.Header.Set(SubjectHeader, claims.Subject)

			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// bearerToken extracts the token from an Authorization header.
func bearerToken(r *http.Request) (string, error) {
	h := r.Header.Get(AuthorizationHeader)
	if h == "" {
		return "", errors.New("no Authorization header")
	}
	// The scheme is case-insensitive per RFC 7235.
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", errors.New("not a bearer token")
	}
	token := strings.TrimSpace(h[len(prefix):])
	if token == "" {
		return "", errors.New("empty bearer token")
	}
	return token, nil
}

// errorBody is the gateway's error response shape, kept uniform so clients can
// parse failures the same way regardless of which middleware produced them.
type errorBody struct {
	Error         string `json:"error"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// unauthorized writes a 401 with a WWW-Authenticate challenge, as RFC 7235
// requires for a 401 response.
func unauthorized(w http.ResponseWriter, r *http.Request, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="api"`)
	writeError(w, r, http.StatusUnauthorized, msg)
}

// writeError sends a JSON error carrying the correlation ID, so a user
// reporting a failure gives support the exact ID to search for.
func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{
		Error:         msg,
		CorrelationID: CorrelationID(r.Context()),
	})
}
