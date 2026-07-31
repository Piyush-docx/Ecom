// Package httpx holds the small HTTP conventions every service follows:
// a uniform JSON error shape, request decoding with a body limit, and the
// trusted-identity header the gateway sets.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"pkg/correlation"
)

// SubjectHeader carries the authenticated user's ID from the gateway.
//
// Services trust this header (IMPLEMENTATION_PLAN.md 1.7): the gateway
// validates the JWT once and forwards the subject. That trust is only sound
// while services are unreachable except through the gateway. Phase 7's compose
// topology must keep service ports off the host network, or anyone could set
// this header and impersonate any user.
const SubjectHeader = "X-User-ID"

// Subject returns the authenticated user ID the gateway attached, or "" if the
// request did not come through the gateway's protected routes.
func Subject(r *http.Request) string {
	return r.Header.Get(SubjectHeader)
}

// ErrorResponse is the uniform error body, so a client can parse a failure the
// same way regardless of which service produced it.
type ErrorResponse struct {
	Error         string `json:"error"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// WriteJSON sends v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// The header and status are already sent, so a marshalling failure here
	// cannot be turned into an error response. Logging it is the caller's job;
	// dropping it silently would at least not corrupt the response further.
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError sends a JSON error carrying the correlation ID, so a user
// reporting a failure hands support the exact ID to search for.
func WriteError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	WriteJSON(w, r, status, ErrorResponse{
		Error:         msg,
		CorrelationID: correlation.FromContext(r.Context()),
	})
}

// maxBodyBytes bounds a request body. Without a limit, a single client could
// exhaust a service's memory by streaming an unbounded JSON document.
const maxBodyBytes = 1 << 20 // 1 MiB

// DecodeJSON reads a JSON request body into v.
//
// It rejects unknown fields so a typo'd field name fails loudly instead of
// being silently ignored — a client sending {"quantiy": 5} should be told, not
// quietly given a zero.
func DecodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("request body exceeds %d bytes", maxBodyBytes)
		}
		return fmt.Errorf("malformed JSON body: %w", err)
	}

	// A second value in the stream means the client sent something like
	// `{...}{...}`, which is not a single JSON document.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("body must contain exactly one JSON object")
	}
	return nil
}
