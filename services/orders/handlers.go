package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"pkg/correlation"
	"pkg/httpx"
)

// API holds the orders service's dependencies.
type API struct {
	store   *Store
	catalog *CatalogClient
	saga    *SagaCoordinator
	logger  *slog.Logger
}

// Routes returns the service's HTTP handler.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(correlation.Middleware)

	r.Get("/healthz", a.health)

	r.Route("/orders", func(r chi.Router) {
		r.Post("/", a.createOrder)
		r.Get("/", a.listOrders)
		r.Get("/{id}", a.getOrder)
	})
	return r
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

type orderItemRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type createOrderRequest struct {
	Items []orderItemRequest `json:"items"`
}

type orderItemView struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	UnitCents int64  `json:"unit_cents"`
}

type orderView struct {
	ID            string          `json:"id"`
	UserID        string          `json:"user_id"`
	Status        string          `json:"status"`
	TotalCents    int64           `json:"total_cents"`
	Currency      string          `json:"currency"`
	FailureReason string          `json:"failure_reason,omitempty"`
	Items         []orderItemView `json:"items"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

func toView(o *Order) orderView {
	items := make([]orderItemView, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, orderItemView{
			ProductID: it.ProductID, Quantity: it.Quantity, UnitCents: it.UnitCents,
		})
	}
	return orderView{
		ID: o.ID, UserID: o.UserID, Status: o.Status,
		TotalCents: o.TotalCents, Currency: o.Currency,
		FailureReason: o.FailureReason, Items: items,
		CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt,
	}
}

// maxItemsPerOrder bounds an order. Without a limit, one request could ask the
// catalog to reserve thousands of lines and hold a transaction open.
const maxItemsPerOrder = 50

// createOrder reserves inventory, then records a pending order.
//
// Reservation happens before the order row exists, so a stock shortage is
// reported as a clean 409 with nothing persisted. In Phase 5 this becomes
// event-driven — orders will publish OrderCreated and let the catalog consume
// it — but the ordering constraint is the same either way: an order must never
// be confirmed against stock that was never held.
func (a *API) createOrder(w http.ResponseWriter, r *http.Request) {
	userID := httpx.Subject(r)
	if userID == "" {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthenticated")
		return
	}

	var req createOrderRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	switch {
	case len(req.Items) == 0:
		httpx.WriteError(w, r, http.StatusBadRequest, "an order must have at least one item")
		return
	case len(req.Items) > maxItemsPerOrder:
		httpx.WriteError(w, r, http.StatusBadRequest, "too many items in one order")
		return
	}

	seen := make(map[string]bool, len(req.Items))
	for _, item := range req.Items {
		if item.ProductID == "" {
			httpx.WriteError(w, r, http.StatusBadRequest, "each item needs a product_id")
			return
		}
		if item.Quantity <= 0 {
			httpx.WriteError(w, r, http.StatusBadRequest, "each item needs a positive quantity")
			return
		}
		// The order_items primary key is (order_id, product_id), so a repeated
		// product would violate it. Rejecting here gives a clear 400 rather
		// than a 500 from the database.
		if seen[item.ProductID] {
			httpx.WriteError(w, r, http.StatusBadRequest, "each product may appear only once per order")
			return
		}
		seen[item.ProductID] = true
	}

	orderID := newUUID()

	// Price each line from the catalog. Trusting a client-supplied price would
	// let anyone buy anything for a cent.
	items := make([]OrderItem, 0, len(req.Items))
	var total int64
	currency := "USD"

	for _, item := range req.Items {
		product, err := a.catalog.Product(r.Context(), item.ProductID)
		if err != nil {
			if errors.Is(err, ErrCatalogNotFound) {
				httpx.WriteError(w, r, http.StatusBadRequest, "unknown product: "+item.ProductID)
				return
			}
			a.logger.ErrorContext(r.Context(), "pricing order item", "error", err, "product_id", item.ProductID)
			httpx.WriteError(w, r, http.StatusBadGateway, "could not price the order")
			return
		}
		currency = product.Currency
		total += product.PriceCents * int64(item.Quantity)
		items = append(items, OrderItem{
			ProductID: item.ProductID, Quantity: item.Quantity, UnitCents: product.PriceCents,
		})
	}

	// Reserve every line before persisting anything. On a shortage, release
	// whatever was already held so a failed order leaves no stock stranded.
	reserved := make([]string, 0, len(items))
	for _, item := range items {
		err := a.catalog.Reserve(r.Context(), orderID, item.ProductID, item.Quantity)
		if err != nil {
			a.releaseAll(r, orderID, reserved)

			if errors.Is(err, ErrCatalogConflict) {
				httpx.WriteError(w, r, http.StatusConflict, "insufficient stock for product "+item.ProductID)
				return
			}
			if errors.Is(err, ErrCatalogNotFound) {
				httpx.WriteError(w, r, http.StatusBadRequest, "unknown product: "+item.ProductID)
				return
			}
			a.logger.ErrorContext(r.Context(), "reserving stock", "error", err, "product_id", item.ProductID)
			httpx.WriteError(w, r, http.StatusBadGateway, "could not reserve stock")
			return
		}
		reserved = append(reserved, item.ProductID)
	}

	order, err := a.store.CreateOrder(r.Context(), Order{
		ID: orderID, UserID: userID, TotalCents: total, Currency: currency, Items: items,
	})
	if err != nil {
		// The order could not be recorded, so the holds must not persist —
		// otherwise that stock is unavailable to anyone with no order to show
		// for it.
		a.releaseAll(r, orderID, reserved)
		a.logger.ErrorContext(r.Context(), "creating order", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not create order")
		return
	}

	a.logger.InfoContext(r.Context(), "order created",
		"order_id", order.ID, "user_id", userID, "total_cents", total)

	// Start the saga. Publishing after the order row exists and its stock is
	// held means payment can never charge for an order the database failed to
	// record.
	//
	// A publish failure is reported as 502 rather than swallowed: the order
	// exists and holds stock, but nothing will ever charge for it, so the
	// client must know its checkout did not complete. The order stays pending
	// and its reservation stays held, which is visible to an operator -- the
	// same tradeoff the compensation path makes, for the same reason.
	if a.saga != nil {
		if err := a.saga.PublishOrderCreated(r.Context(), order); err != nil {
			a.logger.ErrorContext(r.Context(), "publishing OrderCreated failed; the order will not progress",
				"error", err, "order_id", order.ID)
			httpx.WriteError(w, r, http.StatusBadGateway, "order created but could not be submitted for payment")
			return
		}
	}

	httpx.WriteJSON(w, r, http.StatusCreated, toView(order))
}

// releaseAll compensates the reservations made so far.
//
// Failures are logged rather than returned: the caller is already on an error
// path, and the reservation is idempotent, so a retry or an operator can clean
// up. Losing the original error to report a compensation error would be worse.
func (a *API) releaseAll(r *http.Request, orderID string, productIDs []string) {
	for _, productID := range productIDs {
		if err := a.catalog.Release(r.Context(), orderID, productID); err != nil {
			a.logger.ErrorContext(r.Context(), "releasing reservation after a failed order",
				"error", err, "order_id", orderID, "product_id", productID)
		}
	}
}

// getOrder returns one order, and only to its owner.
func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	userID := httpx.Subject(r)
	if userID == "" {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthenticated")
		return
	}

	order, err := a.store.OrderByID(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		if errors.Is(err, ErrOrderNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "order not found")
			return
		}
		a.logger.ErrorContext(r.Context(), "loading order", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not load order")
		return
	}

	// Another user's order is reported as 404, not 403. A 403 would confirm the
	// order exists, letting someone enumerate valid order IDs.
	if order.UserID != userID {
		a.logger.WarnContext(r.Context(), "order access denied",
			"order_id", order.ID, "owner", order.UserID, "requester", userID)
		httpx.WriteError(w, r, http.StatusNotFound, "order not found")
		return
	}

	httpx.WriteJSON(w, r, http.StatusOK, toView(order))
}

// listOrders returns the caller's own orders.
func (a *API) listOrders(w http.ResponseWriter, r *http.Request) {
	userID := httpx.Subject(r)
	if userID == "" {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthenticated")
		return
	}

	limit := intParam(r, "limit", 20, 1, 100)
	offset := intParam(r, "offset", 0, 0, 1_000_000)

	// Scoped to the caller in the query itself, so there is no code path that
	// could return someone else's orders.
	orders, err := a.store.OrdersForUser(r.Context(), userID, limit, offset)
	if err != nil {
		a.logger.ErrorContext(r.Context(), "listing orders", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not list orders")
		return
	}

	views := make([]orderView, 0, len(orders))
	for i := range orders {
		views = append(views, toView(&orders[i]))
	}
	httpx.WriteJSON(w, r, http.StatusOK, map[string]any{
		"orders": views, "limit": limit, "offset": offset,
	})
}

func intParam(r *http.Request, name string, def, min, max int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
