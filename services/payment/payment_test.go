package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"pkg/dbx"
	"pkg/httpx"
)

// Integration tests against a real Postgres. The idempotency guarantee this
// service exists to provide is enforced by a unique index, which a mocked
// database would not exercise.
//
//	docker compose -f deploy/docker-compose.yml up -d postgres

func testDSN() string {
	if v := os.Getenv("PAYMENT_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://ecom:ecom@localhost:5432/ecom_payment?sslmode=disable"
}

func newTestAPI(t *testing.T, gateway Gateway) (*API, *pgxpool.Pool) {
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

	if gateway == nil {
		gateway = StubGateway{}
	}
	return &API{
		store:   NewStore(pool),
		gateway: gateway,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, pool
}

const testUser = "user-under-test"

func doJSON(t *testing.T, api *API, method, path string, body any, userID string) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encoding body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if userID != "" {
		req.Header.Set(httpx.SubjectHeader, userID)
	}
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)
	return rec
}

func decodePayment(t *testing.T, rec *httptest.ResponseRecorder) paymentView {
	t.Helper()
	var p paymentView
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decoding payment: %v (body=%q)", err, rec.Body.String())
	}
	return p
}

// TestChargeSucceeds covers the happy path.
func TestChargeSucceeds(t *testing.T) {
	api, _ := newTestAPI(t, nil)

	orderID := newUUID()
	rec := doJSON(t, api, http.MethodPost, "/payment/charges",
		chargeRequest{OrderID: orderID, AmountCents: 4999}, testUser)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusCreated, rec.Body)
	}
	p := decodePayment(t, rec)
	if p.Status != StatusSucceeded {
		t.Errorf("status = %q, want %q", p.Status, StatusSucceeded)
	}
	if p.AmountCents != 4999 {
		t.Errorf("amount_cents = %d, want 4999", p.AmountCents)
	}
}

// TestChargeIsIdempotent is the property Phase 5's acceptance criteria depend
// on: "a test that publishes OrderCreated twice with the same order ID must
// result in exactly one charge attempt".
//
// Here that is exercised over HTTP; in Phase 5 the same store call sits behind
// a Kafka consumer, where at-least-once delivery makes redelivery certain
// rather than hypothetical.
func TestChargeIsIdempotent(t *testing.T) {
	api, pool := newTestAPI(t, nil)

	orderID := newUUID()
	req := chargeRequest{OrderID: orderID, AmountCents: 4999}

	first := doJSON(t, api, http.MethodPost, "/payment/charges", req, testUser)
	if first.Code != http.StatusCreated {
		t.Fatalf("first charge: status = %d (body=%s)", first.Code, first.Body)
	}
	firstPayment := decodePayment(t, first)

	// Repeat deliveries.
	for i := 2; i <= 4; i++ {
		rec := doJSON(t, api, http.MethodPost, "/payment/charges", req, testUser)
		if rec.Code != http.StatusOK {
			t.Fatalf("charge attempt %d: status = %d, want 200 (body=%s)", i, rec.Code, rec.Body)
		}
		repeat := decodePayment(t, rec)
		if repeat.ID != firstPayment.ID {
			t.Errorf("charge attempt %d returned payment %q, want the original %q — "+
				"the customer was charged twice", i, repeat.ID, firstPayment.ID)
		}
	}

	// The database must hold exactly one row for this order.
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("counting payments: %v", err)
	}
	if count != 1 {
		t.Errorf("%d charge rows for one order, want exactly 1", count)
	}
}

// TestConcurrentChargesAreIdempotent is the version that matters.
//
// The check-before-charge in the handler is not the guarantee — concurrent
// requests can all pass it. Only the unique index on order_id makes "exactly
// one charge" true, and only a real database can demonstrate that.
func TestConcurrentChargesAreIdempotent(t *testing.T) {
	api, pool := newTestAPI(t, nil)

	orderID := newUUID()
	const attempts = 10

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		codes = map[int]int{}
		ids   = map[string]bool{}
	)

	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := doJSON(t, api, http.MethodPost, "/payment/charges",
				chargeRequest{OrderID: orderID, AmountCents: 4999}, testUser)

			mu.Lock()
			defer mu.Unlock()
			codes[rec.Code]++
			if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
				ids[decodePayment(t, rec).ID] = true
			}
		}()
	}
	close(start)
	wg.Wait()

	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("counting payments: %v", err)
	}
	if count != 1 {
		t.Errorf("%d concurrent charge requests produced %d rows, want exactly 1 — "+
			"the customer was charged more than once", attempts, count)
	}

	// Every caller must observe the same payment, or a client would think its
	// own attempt was the one that settled.
	if len(ids) != 1 {
		t.Errorf("callers saw %d distinct payment ids, want 1: %v", len(ids), ids)
	}
	t.Logf("%d concurrent charges: statuses=%v, rows=%d", attempts, codes, count)
}

// TestDeclinedChargeIsRecorded confirms a failure is a recorded outcome rather
// than a lost request. Phase 5's compensating path is driven by this record.
func TestDeclinedChargeIsRecorded(t *testing.T) {
	const declineAmount = 66600
	api, _ := newTestAPI(t, StubGateway{FailAmountCents: declineAmount})

	rec := doJSON(t, api, http.MethodPost, "/payment/charges",
		chargeRequest{OrderID: newUUID(), AmountCents: declineAmount}, testUser)

	// A declined card is a successfully handled request whose outcome is a
	// failure, so the HTTP status is still 2xx.
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusCreated, rec.Body)
	}
	p := decodePayment(t, rec)
	if p.Status != StatusFailed {
		t.Errorf("status = %q, want %q", p.Status, StatusFailed)
	}
	if p.FailureReason == "" {
		t.Error("a declined charge has no failure_reason")
	}
}

// TestDeclinedChargeIsAlsoIdempotent confirms a redelivered event for a
// declined order does not retry the charge, which could succeed the second time
// and charge a customer whose order was already cancelled.
func TestDeclinedChargeIsAlsoIdempotent(t *testing.T) {
	const declineAmount = 66600
	api, pool := newTestAPI(t, StubGateway{FailAmountCents: declineAmount})

	orderID := newUUID()
	req := chargeRequest{OrderID: orderID, AmountCents: declineAmount}

	if rec := doJSON(t, api, http.MethodPost, "/payment/charges", req, testUser); rec.Code != http.StatusCreated {
		t.Fatalf("first charge: status = %d", rec.Code)
	}

	rec := doJSON(t, api, http.MethodPost, "/payment/charges", req, testUser)
	if rec.Code != http.StatusOK {
		t.Fatalf("repeat charge: status = %d, want 200", rec.Code)
	}
	if got := decodePayment(t, rec); got.Status != StatusFailed {
		t.Errorf("repeat charge status = %q, want the original %q", got.Status, StatusFailed)
	}

	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("counting payments: %v", err)
	}
	if count != 1 {
		t.Errorf("%d rows for a redelivered declined order, want 1", count)
	}
}

// TestChargeRequiresAuthentication confirms the gateway header is required.
func TestChargeRequiresAuthentication(t *testing.T) {
	api, _ := newTestAPI(t, nil)

	rec := doJSON(t, api, http.MethodPost, "/payment/charges",
		chargeRequest{OrderID: newUUID(), AmountCents: 100}, "")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestPaymentIsScopedToOwner confirms one user cannot read another's payment.
func TestPaymentIsScopedToOwner(t *testing.T) {
	api, _ := newTestAPI(t, nil)

	orderID := newUUID()
	if rec := doJSON(t, api, http.MethodPost, "/payment/charges",
		chargeRequest{OrderID: orderID, AmountCents: 100}, "alice"); rec.Code != http.StatusCreated {
		t.Fatalf("charge: status = %d", rec.Code)
	}

	if rec := doJSON(t, api, http.MethodGet, "/payment/charges/"+orderID, nil, "alice"); rec.Code != http.StatusOK {
		t.Errorf("owner reading own payment: status = %d, want 200", rec.Code)
	}

	// 404 rather than 403, so payment existence cannot be probed.
	if rec := doJSON(t, api, http.MethodGet, "/payment/charges/"+orderID, nil, "mallory"); rec.Code != http.StatusNotFound {
		t.Errorf("another user reading the payment: status = %d, want 404", rec.Code)
	}
}

// TestChargeValidation covers rejected inputs.
func TestChargeValidation(t *testing.T) {
	api, _ := newTestAPI(t, nil)

	cases := []struct {
		name string
		body chargeRequest
	}{
		{"no order id", chargeRequest{AmountCents: 100}},
		{"zero amount", chargeRequest{OrderID: newUUID(), AmountCents: 0}},
		{"negative amount", chargeRequest{OrderID: newUUID(), AmountCents: -100}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rec := doJSON(t, api, http.MethodPost, "/payment/charges", tc.body, testUser); rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d (body=%s)", rec.Code, http.StatusBadRequest, rec.Body)
			}
		})
	}
}
