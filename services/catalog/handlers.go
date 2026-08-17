package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"pkg/correlation"
	"pkg/httpx"
	"pkg/metrics"
)

// API holds the catalog service's dependencies.
type API struct {
	store   *Store
	metrics *metrics.Metrics
	logger  *slog.Logger
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

	// The gateway proxies /catalog/* without stripping the prefix.
	r.Route("/catalog", func(r chi.Router) {
		r.Get("/products", a.listProducts)
		r.Post("/products", a.createProduct)
		r.Get("/products/{id}", a.getProduct)
		r.Post("/products/{id}/stock", a.addStock)

		// Reservation endpoints. In Phase 5 these are driven by the saga's
		// event consumers rather than by clients directly, but exposing them
		// over HTTP now keeps the orders service testable before Kafka exists.
		r.Post("/reservations", a.reserve)
		r.Post("/reservations/release", a.release)
		r.Post("/reservations/commit", a.commitReservation)
	})
	return r
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

type productView struct {
	ID          string    `json:"id"`
	SKU         string    `json:"sku"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	PriceCents  int64     `json:"price_cents"`
	Currency    string    `json:"currency"`
	Stock       int       `json:"stock"`
	Reserved    int       `json:"reserved"`
	Available   int       `json:"available"`
	CreatedAt   time.Time `json:"created_at"`
}

func toView(p *Product) productView {
	return productView{
		ID: p.ID, SKU: p.SKU, Name: p.Name, Description: p.Description,
		PriceCents: p.PriceCents, Currency: p.Currency,
		Stock: p.Stock, Reserved: p.Reserved, Available: p.Available(),
		CreatedAt: p.CreatedAt,
	}
}

type createProductRequest struct {
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	Description string `json:"description"`
	PriceCents  int64  `json:"price_cents"`
	Currency    string `json:"currency"`
	Stock       int    `json:"stock"`
}

func (a *API) createProduct(w http.ResponseWriter, r *http.Request) {
	var req createProductRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	req.SKU = strings.TrimSpace(req.SKU)
	req.Name = strings.TrimSpace(req.Name)
	switch {
	case req.SKU == "":
		httpx.WriteError(w, r, http.StatusBadRequest, "sku is required")
		return
	case req.Name == "":
		httpx.WriteError(w, r, http.StatusBadRequest, "name is required")
		return
	case req.PriceCents < 0:
		httpx.WriteError(w, r, http.StatusBadRequest, "price_cents must not be negative")
		return
	case req.Stock < 0:
		httpx.WriteError(w, r, http.StatusBadRequest, "stock must not be negative")
		return
	}

	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	if currency == "" {
		currency = "USD"
	}
	if len(currency) != 3 {
		httpx.WriteError(w, r, http.StatusBadRequest, "currency must be a 3-letter code")
		return
	}

	created, err := a.store.CreateProduct(r.Context(), Product{
		ID: newUUID(), SKU: req.SKU, Name: req.Name, Description: req.Description,
		PriceCents: req.PriceCents, Currency: currency, Stock: req.Stock,
	})
	if err != nil {
		if errors.Is(err, ErrSKUTaken) {
			httpx.WriteError(w, r, http.StatusConflict, "sku already exists")
			return
		}
		a.logger.ErrorContext(r.Context(), "creating product", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not create product")
		return
	}

	httpx.WriteJSON(w, r, http.StatusCreated, toView(created))
}

func (a *API) getProduct(w http.ResponseWriter, r *http.Request) {
	p, err := a.store.ProductByID(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		a.writeStoreError(w, r, err, "could not load product")
		return
	}
	httpx.WriteJSON(w, r, http.StatusOK, toView(p))
}

func (a *API) listProducts(w http.ResponseWriter, r *http.Request) {
	// Bounded page size: an unbounded limit lets one request pull the entire
	// table into memory.
	limit := intParam(r, "limit", 50, 1, 200)
	offset := intParam(r, "offset", 0, 0, 1_000_000)

	products, err := a.store.ListProducts(r.Context(), limit, offset)
	if err != nil {
		a.logger.ErrorContext(r.Context(), "listing products", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not list products")
		return
	}

	views := make([]productView, 0, len(products))
	for i := range products {
		views = append(views, toView(&products[i]))
	}
	httpx.WriteJSON(w, r, http.StatusOK, map[string]any{
		"products": views,
		"limit":    limit,
		"offset":   offset,
	})
}

type stockRequest struct {
	Quantity int `json:"quantity"`
}

func (a *API) addStock(w http.ResponseWriter, r *http.Request) {
	var req stockRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if req.Quantity <= 0 {
		httpx.WriteError(w, r, http.StatusBadRequest, "quantity must be positive")
		return
	}

	p, err := a.store.AddStock(r.Context(), chi.URLParam(r, "id"), req.Quantity)
	if err != nil {
		a.writeStoreError(w, r, err, "could not add stock")
		return
	}
	httpx.WriteJSON(w, r, http.StatusOK, toView(p))
}

type reservationRequest struct {
	OrderID   string `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func (a *API) reserve(w http.ResponseWriter, r *http.Request) {
	var req reservationRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if req.OrderID == "" || req.ProductID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "order_id and product_id are required")
		return
	}
	if req.Quantity <= 0 {
		httpx.WriteError(w, r, http.StatusBadRequest, "quantity must be positive")
		return
	}

	p, err := a.store.Reserve(r.Context(), req.OrderID, req.ProductID, req.Quantity)
	if err != nil {
		if errors.Is(err, ErrInsufficientStock) {
			// 409 rather than 400: the request is well-formed, it conflicts
			// with current state.
			httpx.WriteError(w, r, http.StatusConflict, "insufficient stock")
			return
		}
		a.writeStoreError(w, r, err, "could not reserve stock")
		return
	}

	a.logger.InfoContext(r.Context(), "stock reserved",
		"order_id", req.OrderID, "product_id", req.ProductID, "quantity", req.Quantity)
	httpx.WriteJSON(w, r, http.StatusOK, toView(p))
}

func (a *API) release(w http.ResponseWriter, r *http.Request) {
	a.reservationAction(w, r, a.store.Release, "stock released", "could not release stock")
}

func (a *API) commitReservation(w http.ResponseWriter, r *http.Request) {
	a.reservationAction(w, r, a.store.Commit, "stock committed", "could not commit stock")
}

// reservationAction handles the release and commit endpoints, which differ only
// in the store call they make.
func (a *API) reservationAction(
	w http.ResponseWriter,
	r *http.Request,
	action func(ctx context.Context, orderID, productID string) (*Product, error),
	logMsg, errMsg string,
) {
	var req reservationRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if req.OrderID == "" || req.ProductID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "order_id and product_id are required")
		return
	}

	p, err := action(r.Context(), req.OrderID, req.ProductID)
	if err != nil {
		a.writeStoreError(w, r, err, errMsg)
		return
	}

	a.logger.InfoContext(r.Context(), logMsg, "order_id", req.OrderID, "product_id", req.ProductID)
	httpx.WriteJSON(w, r, http.StatusOK, toView(p))
}

// writeStoreError maps a store error to a status, logging anything unexpected.
func (a *API) writeStoreError(w http.ResponseWriter, r *http.Request, err error, msg string) {
	if errors.Is(err, ErrProductNotFound) {
		httpx.WriteError(w, r, http.StatusNotFound, "product not found")
		return
	}
	a.logger.ErrorContext(r.Context(), msg, "error", err)
	httpx.WriteError(w, r, http.StatusInternalServerError, msg)
}

// intParam reads a bounded integer query parameter, falling back to def.
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
