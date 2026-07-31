package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrOrderNotFound means no order matched.
	ErrOrderNotFound = errors.New("order not found")

	// ErrInvalidTransition means the requested status change is not permitted
	// from the order's current state.
	ErrInvalidTransition = errors.New("invalid status transition")
)

// Order statuses. These are the saga's states: an order is created pending,
// and moves to exactly one terminal state.
const (
	StatusPending   = "pending"
	StatusConfirmed = "confirmed"
	StatusCancelled = "cancelled"
)

// Order is a row of the orders table with its items.
type Order struct {
	ID            string
	UserID        string
	Status        string
	TotalCents    int64
	Currency      string
	FailureReason string
	Items         []OrderItem
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// OrderItem is one line of an order.
type OrderItem struct {
	ProductID string
	Quantity  int
	UnitCents int64
}

// Store is the orders service's data access layer.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// CreateOrder inserts an order and its items in one transaction.
//
// The transaction matters: an order without its items is a corrupt record that
// no later step can repair, since nothing else knows what was ordered.
func (s *Store) CreateOrder(ctx context.Context, o Order) (*Order, error) {
	if len(o.Items) == 0 {
		return nil, errors.New("an order must have at least one item")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, total_cents, currency)
		 VALUES ($1, $2, $3, $4, $5)`,
		o.ID, o.UserID, StatusPending, o.TotalCents, o.Currency)
	if err != nil {
		return nil, fmt.Errorf("inserting order: %w", err)
	}

	for _, item := range o.Items {
		_, err = tx.Exec(ctx,
			`INSERT INTO order_items (order_id, product_id, quantity, unit_cents)
			 VALUES ($1, $2, $3, $4)`,
			o.ID, item.ProductID, item.Quantity, item.UnitCents)
		if err != nil {
			return nil, fmt.Errorf("inserting order item: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing order: %w", err)
	}

	return s.OrderByID(ctx, o.ID)
}

// OrderByID returns one order with its items.
func (s *Store) OrderByID(ctx context.Context, id string) (*Order, error) {
	var o Order
	var failureReason *string

	err := s.pool.QueryRow(ctx,
		`SELECT id, user_id, status, total_cents, currency, failure_reason, created_at, updated_at
		 FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.UserID, &o.Status, &o.TotalCents, &o.Currency,
			&failureReason, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, fmt.Errorf("querying order: %w", err)
	}
	if failureReason != nil {
		o.FailureReason = *failureReason
	}

	items, err := s.itemsFor(ctx, id)
	if err != nil {
		return nil, err
	}
	o.Items = items
	return &o, nil
}

func (s *Store) itemsFor(ctx context.Context, orderID string) ([]OrderItem, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT product_id, quantity, unit_cents FROM order_items
		 WHERE order_id = $1 ORDER BY product_id`, orderID)
	if err != nil {
		return nil, fmt.Errorf("querying order items: %w", err)
	}
	defer rows.Close()

	items := make([]OrderItem, 0, 4)
	for rows.Next() {
		var it OrderItem
		if err := rows.Scan(&it.ProductID, &it.Quantity, &it.UnitCents); err != nil {
			return nil, fmt.Errorf("scanning order item: %w", err)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// OrdersForUser returns a user's orders, newest first.
func (s *Store) OrdersForUser(ctx context.Context, userID string, limit, offset int) ([]Order, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, user_id, status, total_cents, currency, failure_reason, created_at, updated_at
		 FROM orders WHERE user_id = $1
		 ORDER BY created_at DESC, id LIMIT $2 OFFSET $3`,
		userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("listing orders: %w", err)
	}
	defer rows.Close()

	orders := make([]Order, 0, limit)
	for rows.Next() {
		var o Order
		var failureReason *string
		if err := rows.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalCents, &o.Currency,
			&failureReason, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning order: %w", err)
		}
		if failureReason != nil {
			o.FailureReason = *failureReason
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Items are fetched per order rather than with a join, to keep each row's
	// items grouped without post-processing. Fine at this page size; if it ever
	// matters, one IN query would replace the loop.
	for i := range orders {
		items, err := s.itemsFor(ctx, orders[i].ID)
		if err != nil {
			return nil, err
		}
		orders[i].Items = items
	}
	return orders, nil
}

// UpdateStatus moves an order to a terminal state.
//
// The transition is guarded in the WHERE clause: only a pending order can
// change status. That makes the update idempotent-safe under the at-least-once
// event delivery of Phase 5 — a redelivered PaymentSucceeded cannot flip an
// order that has already been cancelled, and a late PaymentFailed cannot undo a
// confirmation. Without the guard, the last event to arrive would win, and
// event ordering is not guaranteed.
func (s *Store) UpdateStatus(ctx context.Context, orderID, status, failureReason string) (*Order, error) {
	if status != StatusConfirmed && status != StatusCancelled {
		return nil, fmt.Errorf("%w: %q is not a terminal status", ErrInvalidTransition, status)
	}

	var reason *string
	if failureReason != "" {
		reason = &failureReason
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE orders SET status = $2, failure_reason = $3, updated_at = now()
		 WHERE id = $1 AND status = $4`,
		orderID, status, reason, StatusPending)
	if err != nil {
		return nil, fmt.Errorf("updating order status: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// Either the order does not exist, or it is already terminal. Fetching
		// it distinguishes the two, and lets a redelivered event that finds the
		// order already in the intended state succeed rather than error.
		existing, err := s.OrderByID(ctx, orderID)
		if err != nil {
			return nil, err
		}
		if existing.Status == status {
			return existing, nil
		}
		return nil, fmt.Errorf("%w: order is %s, cannot become %s",
			ErrInvalidTransition, existing.Status, status)
	}

	return s.OrderByID(ctx, orderID)
}
