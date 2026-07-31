package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"pkg/correlation"
	"pkg/httpx"
)

// Charge statuses.
const (
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

var (
	// ErrPaymentNotFound means no payment matched.
	ErrPaymentNotFound = errors.New("payment not found")

	// ErrAlreadyCharged means this order already has a charge attempt. It is
	// not an error condition for the caller — it is the idempotency guarantee
	// working — so handlers return the existing record rather than a failure.
	ErrAlreadyCharged = errors.New("order already charged")
)

const uniqueViolation = "23505"

// Payment is a row of the payments table.
type Payment struct {
	ID            string
	OrderID       string
	UserID        string
	AmountCents   int64
	Currency      string
	Status        string
	FailureReason string
	CreatedAt     time.Time
}

// Store is the payment service's data access layer.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const paymentColumns = `id, order_id, user_id, amount_cents, currency, status, failure_reason, created_at`

func scanPayment(row pgx.Row) (*Payment, error) {
	var p Payment
	var reason *string
	if err := row.Scan(&p.ID, &p.OrderID, &p.UserID, &p.AmountCents, &p.Currency,
		&p.Status, &reason, &p.CreatedAt); err != nil {
		return nil, err
	}
	if reason != nil {
		p.FailureReason = *reason
	}
	return &p, nil
}

// RecordCharge stores a charge attempt.
//
// It returns ErrAlreadyCharged when this order has been charged before, which
// the unique index on order_id enforces. This is the mechanism behind the
// Phase 5 acceptance criterion: publishing OrderCreated twice with the same
// order ID must result in exactly one charge attempt.
func (s *Store) RecordCharge(ctx context.Context, p Payment) (*Payment, error) {
	var reason *string
	if p.FailureReason != "" {
		reason = &p.FailureReason
	}

	created, err := scanPayment(s.pool.QueryRow(ctx,
		`INSERT INTO payments (id, order_id, user_id, amount_cents, currency, status, failure_reason)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING `+paymentColumns,
		p.ID, p.OrderID, p.UserID, p.AmountCents, p.Currency, p.Status, reason))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return nil, ErrAlreadyCharged
		}
		return nil, fmt.Errorf("recording charge: %w", err)
	}
	return created, nil
}

// PaymentByOrderID returns the charge attempt for an order.
func (s *Store) PaymentByOrderID(ctx context.Context, orderID string) (*Payment, error) {
	p, err := scanPayment(s.pool.QueryRow(ctx,
		`SELECT `+paymentColumns+` FROM payments WHERE order_id = $1`, orderID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrPaymentNotFound
		}
		return nil, fmt.Errorf("querying payment: %w", err)
	}
	return p, nil
}

// Gateway is the payment processor abstraction.
//
// IMPLEMENTATION_PLAN.md Phase 4 allows a mocked charge — Stripe test mode or a
// local stub. This interface is the seam: a real Stripe implementation would
// satisfy it without any change to the service around it.
type Gateway interface {
	Charge(ctx context.Context, orderID string, amountCents int64, currency string) error
}

// StubGateway approves every charge except those the caller marks for failure.
//
// Phase 5 needs a deterministic way to force PaymentFailed to test the
// compensating path, so failure is triggered by amount rather than at random:
// a test can ask for a failure without depending on chance.
type StubGateway struct {
	// FailAmountCents, when non-zero, fails any charge for exactly this amount.
	FailAmountCents int64
}

// Charge approves unless the amount matches FailAmountCents.
func (g StubGateway) Charge(_ context.Context, _ string, amountCents int64, _ string) error {
	if g.FailAmountCents != 0 && amountCents == g.FailAmountCents {
		return errors.New("card declined")
	}
	return nil
}

// API holds the payment service's dependencies.
type API struct {
	store   *Store
	gateway Gateway
	logger  *slog.Logger
}

// Routes returns the service's HTTP handler.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(correlation.Middleware)

	r.Get("/healthz", a.health)

	r.Route("/payment", func(r chi.Router) {
		r.Post("/charges", a.charge)
		r.Get("/charges/{orderID}", a.getCharge)
	})
	return r
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

type chargeRequest struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

type paymentView struct {
	ID            string    `json:"id"`
	OrderID       string    `json:"order_id"`
	AmountCents   int64     `json:"amount_cents"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

func toView(p *Payment) paymentView {
	return paymentView{
		ID: p.ID, OrderID: p.OrderID, AmountCents: p.AmountCents,
		Currency: p.Currency, Status: p.Status,
		FailureReason: p.FailureReason, CreatedAt: p.CreatedAt,
	}
}

// charge attempts a payment for an order, at most once.
//
// The idempotency contract: calling this repeatedly for one order ID performs
// exactly one charge attempt and returns the same result every time. That
// property is what makes it safe to consume an at-least-once event stream in
// Phase 5.
func (a *API) charge(w http.ResponseWriter, r *http.Request) {
	userID := httpx.Subject(r)
	if userID == "" {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthenticated")
		return
	}

	var req chargeRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if req.OrderID == "" {
		httpx.WriteError(w, r, http.StatusBadRequest, "order_id is required")
		return
	}
	if req.AmountCents <= 0 {
		httpx.WriteError(w, r, http.StatusBadRequest, "amount_cents must be positive")
		return
	}
	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	if currency == "" {
		currency = "USD"
	}

	// Check before charging. This is not the idempotency guarantee — two
	// concurrent requests can both pass it — but it avoids calling the payment
	// processor for an order already settled, which matters when the processor
	// is real and charges money.
	if existing, err := a.store.PaymentByOrderID(r.Context(), req.OrderID); err == nil {
		a.logger.InfoContext(r.Context(), "charge already recorded, returning existing",
			"order_id", req.OrderID, "status", existing.Status)
		httpx.WriteJSON(w, r, http.StatusOK, toView(existing))
		return
	} else if !errors.Is(err, ErrPaymentNotFound) {
		a.logger.ErrorContext(r.Context(), "looking up payment", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not process payment")
		return
	}

	status := StatusSucceeded
	failureReason := ""
	if err := a.gateway.Charge(r.Context(), req.OrderID, req.AmountCents, currency); err != nil {
		status = StatusFailed
		failureReason = err.Error()
	}

	// The insert is the real idempotency boundary: the unique index on
	// order_id rejects a second attempt even if two requests raced past the
	// check above.
	payment, err := a.store.RecordCharge(r.Context(), Payment{
		ID: newUUID(), OrderID: req.OrderID, UserID: userID,
		AmountCents: req.AmountCents, Currency: currency,
		Status: status, FailureReason: failureReason,
	})
	if err != nil {
		if errors.Is(err, ErrAlreadyCharged) {
			// Another request won the race. Return its result so both callers
			// see the same outcome.
			existing, lookupErr := a.store.PaymentByOrderID(r.Context(), req.OrderID)
			if lookupErr != nil {
				a.logger.ErrorContext(r.Context(), "loading the winning charge", "error", lookupErr)
				httpx.WriteError(w, r, http.StatusInternalServerError, "could not process payment")
				return
			}
			httpx.WriteJSON(w, r, http.StatusOK, toView(existing))
			return
		}
		a.logger.ErrorContext(r.Context(), "recording charge", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not process payment")
		return
	}

	a.logger.InfoContext(r.Context(), "charge attempted",
		"order_id", payment.OrderID, "status", payment.Status, "amount_cents", payment.AmountCents)

	// A declined card is a successfully processed request whose outcome is a
	// failure, so 201 with status "failed" rather than an HTTP error. The saga
	// in Phase 5 branches on the body, not the status code.
	httpx.WriteJSON(w, r, http.StatusCreated, toView(payment))
}

// getCharge returns the charge attempt for an order.
func (a *API) getCharge(w http.ResponseWriter, r *http.Request) {
	userID := httpx.Subject(r)
	if userID == "" {
		httpx.WriteError(w, r, http.StatusUnauthorized, "unauthenticated")
		return
	}

	payment, err := a.store.PaymentByOrderID(r.Context(), chi.URLParam(r, "orderID"))
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			httpx.WriteError(w, r, http.StatusNotFound, "payment not found")
			return
		}
		a.logger.ErrorContext(r.Context(), "loading payment", "error", err)
		httpx.WriteError(w, r, http.StatusInternalServerError, "could not load payment")
		return
	}

	// Another user's payment is a 404, not a 403, so payment IDs cannot be
	// enumerated.
	if payment.UserID != userID {
		httpx.WriteError(w, r, http.StatusNotFound, "payment not found")
		return
	}

	httpx.WriteJSON(w, r, http.StatusOK, toView(payment))
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
