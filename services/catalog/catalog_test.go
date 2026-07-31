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

	"github.com/jackc/pgx/v5/pgxpool"

	"pkg/dbx"
)

// Integration tests against a real Postgres. The reservation logic is the
// reason IMPLEMENTATION_PLAN.md Phase 4 forbids mocking the DB layer: the
// protection against overselling lives in a SQL WHERE clause and a CHECK
// constraint, neither of which a mock would exercise.
//
//	docker compose -f deploy/docker-compose.yml up -d postgres

func testDSN() string {
	if v := os.Getenv("CATALOG_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://ecom:ecom@localhost:5432/ecom_catalog?sslmode=disable"
}

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
	}, pool
}

func uniqueSKU(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("SKU-%s-%d",
		strings.ToUpper(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano())
}

func doJSON(t *testing.T, api *API, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encoding body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)
	return rec
}

func decodeProduct(t *testing.T, rec *httptest.ResponseRecorder) productView {
	t.Helper()
	var p productView
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decoding product: %v (body=%q)", err, rec.Body.String())
	}
	return p
}

// createProduct is a helper that makes a product with the given stock.
func createProduct(t *testing.T, api *API, stock int) productView {
	t.Helper()
	rec := doJSON(t, api, http.MethodPost, "/catalog/products", createProductRequest{
		SKU: uniqueSKU(t), Name: "Test Product", PriceCents: 1999, Stock: stock,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("creating product: status = %d (body=%s)", rec.Code, rec.Body)
	}
	return decodeProduct(t, rec)
}

// TestProductCRUD covers the basic lifecycle.
func TestProductCRUD(t *testing.T) {
	api, _ := newTestAPI(t)

	sku := uniqueSKU(t)
	rec := doJSON(t, api, http.MethodPost, "/catalog/products", createProductRequest{
		SKU: sku, Name: "Widget", Description: "A widget", PriceCents: 2500, Stock: 10,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d (body=%s)", rec.Code, rec.Body)
	}

	created := decodeProduct(t, rec)
	if created.SKU != sku {
		t.Errorf("sku = %q, want %q", created.SKU, sku)
	}
	if created.PriceCents != 2500 {
		t.Errorf("price_cents = %d, want 2500", created.PriceCents)
	}
	if created.Currency != "USD" {
		t.Errorf("currency = %q, want the USD default", created.Currency)
	}
	if created.Available != 10 {
		t.Errorf("available = %d, want 10", created.Available)
	}

	rec = doJSON(t, api, http.MethodGet, "/catalog/products/"+created.ID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: status = %d", rec.Code)
	}
	if got := decodeProduct(t, rec); got.ID != created.ID {
		t.Errorf("get returned id %q, want %q", got.ID, created.ID)
	}

	rec = doJSON(t, api, http.MethodGet, "/catalog/products", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: status = %d", rec.Code)
	}
	var list struct {
		Products []productView `json:"products"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decoding list: %v", err)
	}
	if len(list.Products) == 0 {
		t.Error("list returned no products")
	}
}

// TestDuplicateSKURejected confirms the unique index is enforced.
func TestDuplicateSKURejected(t *testing.T) {
	api, _ := newTestAPI(t)

	body := createProductRequest{SKU: uniqueSKU(t), Name: "Widget", PriceCents: 100, Stock: 1}
	if rec := doJSON(t, api, http.MethodPost, "/catalog/products", body); rec.Code != http.StatusCreated {
		t.Fatalf("first create: status = %d", rec.Code)
	}
	if rec := doJSON(t, api, http.MethodPost, "/catalog/products", body); rec.Code != http.StatusConflict {
		t.Errorf("duplicate sku: status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

// TestReserveDoesNotOversellUnderConcurrency is the reason this service's tests
// run against a real database.
//
// Twenty goroutines each try to reserve one unit of a product with ten in
// stock. Exactly ten must succeed.
//
// There are two independent protections, and the race is real: with both the
// WHERE-clause check and the reserved_within_stock CHECK constraint removed,
// this scenario reserves 18 units against 10 in stock — measured, not
// hypothesized. Removing only one of the two still yields the correct answer,
// because either alone is sufficient. That redundancy is deliberate: the
// application check gives a clean 409, and the constraint is the last line of
// defense against any future code path that forgets it.
//
// This is the same check-then-act race the rate limiter rules out in Phase 2,
// and the reason the plan forbids mocking the DB layer.
func TestReserveDoesNotOversellUnderConcurrency(t *testing.T) {
	api, _ := newTestAPI(t)

	const (
		stock    = 10
		attempts = 20
	)
	product := createProduct(t, api, stock)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		reserved  int
		conflicts int
		other     []int
	)

	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			// A distinct order per goroutine: the idempotency key is
			// (order_id, product_id), so reusing one order would make all but
			// the first a no-op and hide the race.
			rec := doJSON(t, api, http.MethodPost, "/catalog/reservations", reservationRequest{
				OrderID: newUUID(), ProductID: product.ID, Quantity: 1,
			})

			mu.Lock()
			defer mu.Unlock()
			switch rec.Code {
			case http.StatusOK:
				reserved++
			case http.StatusConflict:
				conflicts++
			default:
				other = append(other, rec.Code)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(other) > 0 {
		t.Errorf("unexpected statuses %v, want only 200 and 409", other)
	}
	if reserved != stock {
		t.Errorf("%d concurrent reservations against %d stock succeeded %d times, want exactly %d — "+
			"the inventory was oversold", attempts, stock, reserved, stock)
	}
	if conflicts != attempts-stock {
		t.Errorf("got %d conflicts, want %d", conflicts, attempts-stock)
	}

	// The stored row must agree with the responses.
	rec := doJSON(t, api, http.MethodGet, "/catalog/products/"+product.ID, nil)
	final := decodeProduct(t, rec)
	if final.Reserved != stock {
		t.Errorf("stored reserved = %d, want %d", final.Reserved, stock)
	}
	if final.Available != 0 {
		t.Errorf("stored available = %d, want 0", final.Available)
	}
}

// TestReserveIsIdempotent confirms a redelivered reservation for the same order
// does not place a second hold.
//
// Phase 5's Kafka delivery is at-least-once, so this is not hypothetical: the
// same OrderCreated will eventually arrive twice, and a second hold would
// silently consume stock nobody ordered.
func TestReserveIsIdempotent(t *testing.T) {
	api, _ := newTestAPI(t)

	product := createProduct(t, api, 10)
	orderID := newUUID()
	req := reservationRequest{OrderID: orderID, ProductID: product.ID, Quantity: 3}

	for i := 1; i <= 3; i++ {
		rec := doJSON(t, api, http.MethodPost, "/catalog/reservations", req)
		if rec.Code != http.StatusOK {
			t.Fatalf("reservation attempt %d: status = %d (body=%s)", i, rec.Code, rec.Body)
		}
		got := decodeProduct(t, rec)
		if got.Reserved != 3 {
			t.Errorf("after %d identical reservations, reserved = %d, want 3 — "+
				"a redelivered event double-reserved", i, got.Reserved)
		}
	}
}

// TestReleaseReturnsStock covers the saga's compensating action: a failed
// payment must give the held stock back.
func TestReleaseReturnsStock(t *testing.T) {
	api, _ := newTestAPI(t)

	product := createProduct(t, api, 10)
	orderID := newUUID()

	rec := doJSON(t, api, http.MethodPost, "/catalog/reservations", reservationRequest{
		OrderID: orderID, ProductID: product.ID, Quantity: 4,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("reserve: status = %d (body=%s)", rec.Code, rec.Body)
	}
	if got := decodeProduct(t, rec); got.Available != 6 {
		t.Fatalf("after reserving 4 of 10, available = %d, want 6", got.Available)
	}

	rec = doJSON(t, api, http.MethodPost, "/catalog/reservations/release", reservationRequest{
		OrderID: orderID, ProductID: product.ID,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("release: status = %d (body=%s)", rec.Code, rec.Body)
	}

	got := decodeProduct(t, rec)
	if got.Reserved != 0 {
		t.Errorf("after release, reserved = %d, want 0", got.Reserved)
	}
	if got.Available != 10 {
		t.Errorf("after release, available = %d, want the original 10", got.Available)
	}
	if got.Stock != 10 {
		t.Errorf("release changed physical stock to %d, want 10 — "+
			"a release must return the hold, not consume inventory", got.Stock)
	}
}

// TestReleaseIsIdempotent confirms a repeated compensating action is safe.
//
// IMPLEMENTATION_PLAN.md 5 asks what happens if the compensating action itself
// fails; the answer is that it can be retried, which is only true if repeating
// it cannot corrupt state.
func TestReleaseIsIdempotent(t *testing.T) {
	api, _ := newTestAPI(t)

	product := createProduct(t, api, 10)
	orderID := newUUID()

	doJSON(t, api, http.MethodPost, "/catalog/reservations", reservationRequest{
		OrderID: orderID, ProductID: product.ID, Quantity: 4,
	})

	for i := 1; i <= 3; i++ {
		rec := doJSON(t, api, http.MethodPost, "/catalog/reservations/release", reservationRequest{
			OrderID: orderID, ProductID: product.ID,
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("release attempt %d: status = %d (body=%s)", i, rec.Code, rec.Body)
		}
		got := decodeProduct(t, rec)
		if got.Available != 10 {
			t.Errorf("after %d releases, available = %d, want 10 — "+
				"a repeated release corrupted the count", i, got.Available)
		}
		if got.Stock != 10 {
			t.Errorf("after %d releases, stock = %d, want 10", i, got.Stock)
		}
	}
}

// TestCommitConsumesStock covers the success path: a confirmed order turns the
// hold into a permanent decrement.
func TestCommitConsumesStock(t *testing.T) {
	api, _ := newTestAPI(t)

	product := createProduct(t, api, 10)
	orderID := newUUID()

	doJSON(t, api, http.MethodPost, "/catalog/reservations", reservationRequest{
		OrderID: orderID, ProductID: product.ID, Quantity: 4,
	})

	rec := doJSON(t, api, http.MethodPost, "/catalog/reservations/commit", reservationRequest{
		OrderID: orderID, ProductID: product.ID,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("commit: status = %d (body=%s)", rec.Code, rec.Body)
	}

	got := decodeProduct(t, rec)
	if got.Stock != 6 {
		t.Errorf("after committing 4 of 10, stock = %d, want 6", got.Stock)
	}
	if got.Reserved != 0 {
		t.Errorf("after commit, reserved = %d, want 0", got.Reserved)
	}
	if got.Available != 6 {
		t.Errorf("after commit, available = %d, want 6", got.Available)
	}

	// Repeating the commit must not consume more stock.
	rec = doJSON(t, api, http.MethodPost, "/catalog/reservations/commit", reservationRequest{
		OrderID: orderID, ProductID: product.ID,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("repeat commit: status = %d", rec.Code)
	}
	if got := decodeProduct(t, rec); got.Stock != 6 {
		t.Errorf("after a repeated commit, stock = %d, want 6 — the commit is not idempotent", got.Stock)
	}
}

// TestReserveBeyondStockIsRejected confirms a single oversized request fails.
func TestReserveBeyondStockIsRejected(t *testing.T) {
	api, _ := newTestAPI(t)

	product := createProduct(t, api, 5)

	rec := doJSON(t, api, http.MethodPost, "/catalog/reservations", reservationRequest{
		OrderID: newUUID(), ProductID: product.ID, Quantity: 6,
	})
	if rec.Code != http.StatusConflict {
		t.Errorf("reserving 6 of 5: status = %d, want %d", rec.Code, http.StatusConflict)
	}

	// Nothing should have been held.
	got := decodeProduct(t, doJSON(t, api, http.MethodGet, "/catalog/products/"+product.ID, nil))
	if got.Reserved != 0 {
		t.Errorf("after a rejected reservation, reserved = %d, want 0", got.Reserved)
	}
}

// TestStockNeverGoesNegative confirms the database constraint holds even if the
// application logic were bypassed. The CHECK is the last line of defense.
func TestStockNeverGoesNegative(t *testing.T) {
	_, pool := newTestAPI(t)

	ctx := context.Background()
	id := newUUID()
	_, err := pool.Exec(ctx,
		`INSERT INTO products (id, sku, name, price_cents, stock, reserved) VALUES ($1, $2, 'x', 100, 5, 0)`,
		id, uniqueSKU(t))
	if err != nil {
		t.Fatalf("inserting product: %v", err)
	}

	// Attempt to reserve more than exists by writing the row directly.
	_, err = pool.Exec(ctx, `UPDATE products SET reserved = 10 WHERE id = $1`, id)
	if err == nil {
		t.Error("the database allowed reserved to exceed stock — the reserved_within_stock CHECK is missing")
	}

	_, err = pool.Exec(ctx, `UPDATE products SET stock = -1 WHERE id = $1`, id)
	if err == nil {
		t.Error("the database allowed negative stock — the stock CHECK is missing")
	}
}

// TestValidation covers rejected inputs.
func TestValidation(t *testing.T) {
	api, _ := newTestAPI(t)

	t.Run("create", func(t *testing.T) {
		cases := []struct {
			name string
			body createProductRequest
		}{
			{"no sku", createProductRequest{Name: "x", PriceCents: 1}},
			{"no name", createProductRequest{SKU: "S1", PriceCents: 1}},
			{"negative price", createProductRequest{SKU: "S1", Name: "x", PriceCents: -1}},
			{"negative stock", createProductRequest{SKU: "S1", Name: "x", PriceCents: 1, Stock: -1}},
			{"bad currency", createProductRequest{SKU: "S1", Name: "x", PriceCents: 1, Currency: "DOLLARS"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if rec := doJSON(t, api, http.MethodPost, "/catalog/products", tc.body); rec.Code != http.StatusBadRequest {
					t.Errorf("status = %d, want %d (body=%s)", rec.Code, http.StatusBadRequest, rec.Body)
				}
			})
		}
	})

	t.Run("reserve", func(t *testing.T) {
		product := createProduct(t, api, 5)
		cases := []struct {
			name string
			body reservationRequest
		}{
			{"no order id", reservationRequest{ProductID: product.ID, Quantity: 1}},
			{"no product id", reservationRequest{OrderID: newUUID(), Quantity: 1}},
			{"zero quantity", reservationRequest{OrderID: newUUID(), ProductID: product.ID, Quantity: 0}},
			{"negative quantity", reservationRequest{OrderID: newUUID(), ProductID: product.ID, Quantity: -1}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if rec := doJSON(t, api, http.MethodPost, "/catalog/reservations", tc.body); rec.Code != http.StatusBadRequest {
					t.Errorf("status = %d, want %d (body=%s)", rec.Code, http.StatusBadRequest, rec.Body)
				}
			})
		}
	})
}

// TestUnknownProductReturns404 confirms a missing product is a 404 rather than
// a 500.
func TestUnknownProductReturns404(t *testing.T) {
	api, _ := newTestAPI(t)

	missing := newUUID()

	if rec := doJSON(t, api, http.MethodGet, "/catalog/products/"+missing, nil); rec.Code != http.StatusNotFound {
		t.Errorf("get unknown product: status = %d, want 404", rec.Code)
	}
	if rec := doJSON(t, api, http.MethodPost, "/catalog/reservations", reservationRequest{
		OrderID: newUUID(), ProductID: missing, Quantity: 1,
	}); rec.Code != http.StatusNotFound {
		t.Errorf("reserve unknown product: status = %d, want 404", rec.Code)
	}
}
