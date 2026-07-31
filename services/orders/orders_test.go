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

// Integration tests against a real Postgres.
//
// The catalog is a stub here rather than the real service. That is a
// deliberate boundary: these tests are about the orders service's own
// behavior — pricing, ownership, compensation on failure — and the catalog's
// inventory correctness has its own integration tests against its own
// database. Wiring the two real services together is the end-to-end test in
// Phase 4's acceptance criteria, run separately.
//
//	docker compose -f deploy/docker-compose.yml up -d postgres

func testDSN() string {
	if v := os.Getenv("ORDERS_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://ecom:ecom@localhost:5432/ecom_orders?sslmode=disable"
}

const testUser = "user-under-test"

// stubCatalog is a configurable in-process catalog.
type stubCatalog struct {
	*httptest.Server

	mu sync.Mutex
	// price per product ID; a product absent from the map is a 404.
	prices map[string]int64
	// products whose reservation should fail with 409.
	outOfStock map[string]bool
	// records what was reserved and released, so a test can assert that a
	// failed order left no stock held.
	reserved map[string]int
	released map[string]int
}

func newStubCatalog(t *testing.T) *stubCatalog {
	t.Helper()

	s := &stubCatalog{
		prices:     map[string]int64{},
		outOfStock: map[string]bool{},
		reserved:   map[string]int{},
		released:   map[string]int{},
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /catalog/products/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		price, ok := s.prices[r.PathValue("id")]
		s.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": r.PathValue("id"), "price_cents": price, "currency": "USD", "available": 100,
		})
	})

	mux.HandleFunc("POST /catalog/reservations", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OrderID   string `json:"order_id"`
			ProductID string `json:"product_id"`
			Quantity  int    `json:"quantity"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, known := s.prices[req.ProductID]; !known {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if s.outOfStock[req.ProductID] {
			w.WriteHeader(http.StatusConflict)
			return
		}
		s.reserved[req.ProductID] += req.Quantity
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	mux.HandleFunc("POST /catalog/reservations/release", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ProductID string `json:"product_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		s.mu.Lock()
		defer s.mu.Unlock()
		s.released[req.ProductID]++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func (s *stubCatalog) addProduct(id string, priceCents int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prices[id] = priceCents
}

func (s *stubCatalog) setOutOfStock(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outOfStock[id] = true
}

func (s *stubCatalog) releaseCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released[id]
}

func newTestAPI(t *testing.T) (*API, *stubCatalog, *pgxpool.Pool) {
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

	catalog := newStubCatalog(t)
	return &API{
		store:   NewStore(pool),
		catalog: NewCatalogClient(catalog.URL),
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, catalog, pool
}

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

func decodeOrder(t *testing.T, rec *httptest.ResponseRecorder) orderView {
	t.Helper()
	var o orderView
	if err := json.Unmarshal(rec.Body.Bytes(), &o); err != nil {
		t.Fatalf("decoding order: %v (body=%q)", err, rec.Body.String())
	}
	return o
}

// TestCreateOrder covers the primary flow: an order is priced from the catalog,
// stock is reserved, and the order is recorded as pending.
func TestCreateOrder(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 2500)

	rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{{ProductID: productID, Quantity: 3}},
	}, testUser)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusCreated, rec.Body)
	}

	order := decodeOrder(t, rec)
	if order.Status != StatusPending {
		t.Errorf("status = %q, want %q — a new order must start pending for the saga", order.Status, StatusPending)
	}
	if order.TotalCents != 7500 {
		t.Errorf("total_cents = %d, want 7500 (3 x 2500)", order.TotalCents)
	}
	if len(order.Items) != 1 {
		t.Fatalf("got %d items, want 1", len(order.Items))
	}
	if order.Items[0].UnitCents != 2500 {
		t.Errorf("unit_cents = %d, want 2500 — the price must be captured at order time", order.Items[0].UnitCents)
	}
}

// TestPriceComesFromCatalogNotClient confirms the client cannot dictate price.
// Trusting a client-supplied price would let anyone buy anything for a cent.
func TestPriceComesFromCatalogNotClient(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 9999)

	// The request type has no price field at all, so the only way a client
	// could influence price is if the service read one. Sending an extra field
	// must be rejected outright by the strict decoder.
	body := []byte(`{"items":[{"product_id":"` + productID + `","quantity":1,"unit_cents":1}]}`)
	req := httptest.NewRequest(http.MethodPost, "/orders/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(httpx.SubjectHeader, testUser)
	rec := httptest.NewRecorder()
	api.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("a request carrying a price field: status = %d, want 400 — "+
			"unknown fields must be rejected, not silently ignored", rec.Code)
	}

	// And the legitimate path prices from the catalog.
	rec = doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{{ProductID: productID, Quantity: 1}},
	}, testUser)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d (body=%s)", rec.Code, rec.Body)
	}
	if got := decodeOrder(t, rec); got.TotalCents != 9999 {
		t.Errorf("total_cents = %d, want the catalog's 9999", got.TotalCents)
	}
}

// TestInsufficientStockLeavesNoOrder confirms a shortage is reported cleanly
// and nothing is persisted.
func TestInsufficientStockLeavesNoOrder(t *testing.T) {
	api, catalog, pool := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 1000)
	catalog.setOutOfStock(productID)

	rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{{ProductID: productID, Quantity: 1}},
	}, testUser)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}

	// Scoped to a user unique to this test: testUser is shared with other tests
	// in the same package, whose orders would otherwise be counted here.
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM orders o
		 JOIN order_items i ON i.order_id = o.id
		 WHERE i.product_id = $1`, productID).Scan(&count); err != nil {
		t.Fatalf("counting orders: %v", err)
	}
	if count != 0 {
		t.Errorf("a rejected order left %d rows, want 0", count)
	}
}

// TestPartialReservationFailureReleasesHolds is the compensation case.
//
// An order with two items where the second is out of stock must release the
// first item's hold. Without that, stock would be held indefinitely for an
// order that was never created — invisible inventory loss.
func TestPartialReservationFailureReleasesHolds(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	inStock := newUUID()
	outOfStock := newUUID()
	catalog.addProduct(inStock, 1000)
	catalog.addProduct(outOfStock, 2000)
	catalog.setOutOfStock(outOfStock)

	rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{
			{ProductID: inStock, Quantity: 1},
			{ProductID: outOfStock, Quantity: 1},
		},
	}, testUser)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusConflict, rec.Body)
	}

	if got := catalog.releaseCount(inStock); got != 1 {
		t.Errorf("the successfully reserved item was released %d times, want 1 — "+
			"a failed order stranded held stock", got)
	}
}

// TestOrderIsScopedToOwner confirms one user cannot read another's order.
func TestOrderIsScopedToOwner(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 1000)

	rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{{ProductID: productID, Quantity: 1}},
	}, "alice")
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d", rec.Code)
	}
	orderID := decodeOrder(t, rec).ID

	if rec := doJSON(t, api, http.MethodGet, "/orders/"+orderID, nil, "alice"); rec.Code != http.StatusOK {
		t.Errorf("owner reading own order: status = %d, want 200", rec.Code)
	}

	// 404 rather than 403: a 403 would confirm the order exists, letting
	// someone enumerate valid order IDs.
	if rec := doJSON(t, api, http.MethodGet, "/orders/"+orderID, nil, "mallory"); rec.Code != http.StatusNotFound {
		t.Errorf("another user reading the order: status = %d, want 404", rec.Code)
	}
}

// TestListOrdersIsScopedToOwner confirms the listing cannot leak other users'
// orders.
func TestListOrdersIsScopedToOwner(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 1000)

	alice := "alice-" + newUUID()
	bob := "bob-" + newUUID()

	for _, user := range []string{alice, alice, bob} {
		if rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
			Items: []orderItemRequest{{ProductID: productID, Quantity: 1}},
		}, user); rec.Code != http.StatusCreated {
			t.Fatalf("creating order for %s: status = %d", user, rec.Code)
		}
	}

	rec := doJSON(t, api, http.MethodGet, "/orders/", nil, alice)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: status = %d", rec.Code)
	}

	var list struct {
		Orders []orderView `json:"orders"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decoding list: %v", err)
	}
	if len(list.Orders) != 2 {
		t.Errorf("alice sees %d orders, want 2", len(list.Orders))
	}
	for _, o := range list.Orders {
		if o.UserID != alice {
			t.Errorf("alice's listing contains an order owned by %q", o.UserID)
		}
	}
}

// TestStatusTransitions covers the saga's state machine, including the guards
// that make it safe under at-least-once event delivery.
func TestStatusTransitions(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 1000)

	newOrder := func(t *testing.T) string {
		t.Helper()
		rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
			Items: []orderItemRequest{{ProductID: productID, Quantity: 1}},
		}, testUser)
		if rec.Code != http.StatusCreated {
			t.Fatalf("creating order: status = %d", rec.Code)
		}
		return decodeOrder(t, rec).ID
	}

	ctx := context.Background()

	t.Run("pending to confirmed", func(t *testing.T) {
		id := newOrder(t)
		o, err := api.store.UpdateStatus(ctx, id, StatusConfirmed, "")
		if err != nil {
			t.Fatalf("confirming: %v", err)
		}
		if o.Status != StatusConfirmed {
			t.Errorf("status = %q, want %q", o.Status, StatusConfirmed)
		}
	})

	t.Run("pending to cancelled records the reason", func(t *testing.T) {
		id := newOrder(t)
		o, err := api.store.UpdateStatus(ctx, id, StatusCancelled, "card declined")
		if err != nil {
			t.Fatalf("cancelling: %v", err)
		}
		if o.Status != StatusCancelled {
			t.Errorf("status = %q, want %q", o.Status, StatusCancelled)
		}
		if o.FailureReason != "card declined" {
			t.Errorf("failure_reason = %q, want %q", o.FailureReason, "card declined")
		}
	})

	t.Run("repeating a transition is idempotent", func(t *testing.T) {
		// A redelivered PaymentSucceeded must not error, or a consumer would
		// retry forever.
		id := newOrder(t)
		if _, err := api.store.UpdateStatus(ctx, id, StatusConfirmed, ""); err != nil {
			t.Fatalf("first confirm: %v", err)
		}
		o, err := api.store.UpdateStatus(ctx, id, StatusConfirmed, "")
		if err != nil {
			t.Errorf("repeating a confirm returned %v, want success — "+
				"a redelivered event would retry forever", err)
		}
		if o != nil && o.Status != StatusConfirmed {
			t.Errorf("status = %q, want %q", o.Status, StatusConfirmed)
		}
	})

	t.Run("a terminal order cannot be flipped", func(t *testing.T) {
		// Event ordering is not guaranteed. A late PaymentFailed arriving after
		// a confirmation must not cancel a paid order.
		id := newOrder(t)
		if _, err := api.store.UpdateStatus(ctx, id, StatusConfirmed, ""); err != nil {
			t.Fatalf("confirming: %v", err)
		}
		if _, err := api.store.UpdateStatus(ctx, id, StatusCancelled, "late failure"); err == nil {
			t.Error("cancelling a confirmed order succeeded, want a rejection — " +
				"a late event could undo a completed payment")
		}
	})

	t.Run("unknown order", func(t *testing.T) {
		if _, err := api.store.UpdateStatus(ctx, newUUID(), StatusConfirmed, ""); err == nil {
			t.Error("updating an unknown order succeeded, want ErrOrderNotFound")
		}
	})
}

// TestCreateOrderRequiresAuthentication confirms the gateway header is needed.
func TestCreateOrderRequiresAuthentication(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 1000)

	rec := doJSON(t, api, http.MethodPost, "/orders/", createOrderRequest{
		Items: []orderItemRequest{{ProductID: productID, Quantity: 1}},
	}, "")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestOrderValidation covers rejected inputs.
func TestOrderValidation(t *testing.T) {
	api, catalog, _ := newTestAPI(t)

	productID := newUUID()
	catalog.addProduct(productID, 1000)

	cases := []struct {
		name string
		body createOrderRequest
		want int
	}{
		{"no items", createOrderRequest{Items: []orderItemRequest{}}, http.StatusBadRequest},
		{"no product id", createOrderRequest{Items: []orderItemRequest{{Quantity: 1}}}, http.StatusBadRequest},
		{"zero quantity", createOrderRequest{Items: []orderItemRequest{{ProductID: productID, Quantity: 0}}}, http.StatusBadRequest},
		{"negative quantity", createOrderRequest{Items: []orderItemRequest{{ProductID: productID, Quantity: -1}}}, http.StatusBadRequest},
		{"unknown product", createOrderRequest{Items: []orderItemRequest{{ProductID: newUUID(), Quantity: 1}}}, http.StatusBadRequest},
		{"duplicate product", createOrderRequest{Items: []orderItemRequest{
			{ProductID: productID, Quantity: 1}, {ProductID: productID, Quantity: 2},
		}}, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rec := doJSON(t, api, http.MethodPost, "/orders/", tc.body, testUser); rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body=%s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}
