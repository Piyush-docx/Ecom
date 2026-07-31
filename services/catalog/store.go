package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrProductNotFound means no product matched.
	ErrProductNotFound = errors.New("product not found")

	// ErrSKUTaken means the unique index rejected the insert.
	ErrSKUTaken = errors.New("sku already exists")

	// ErrInsufficientStock means the requested quantity exceeds what is
	// available (stock minus what is already reserved).
	ErrInsufficientStock = errors.New("insufficient stock")
)

// Product is a row of the products table.
type Product struct {
	ID          string
	SKU         string
	Name        string
	Description string
	PriceCents  int64
	Currency    string
	Stock       int
	Reserved    int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Available is the quantity that can still be promised to a new order.
func (p Product) Available() int { return p.Stock - p.Reserved }

// Store is the catalog service's data access layer.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Postgres SQLSTATE codes the store translates into domain errors.
const (
	uniqueViolation     = "23505"
	foreignKeyViolation = "23503"
	checkViolation      = "23514"
)

const productColumns = `id, sku, name, description, price_cents, currency, stock, reserved, created_at, updated_at`

func scanProduct(row pgx.Row) (*Product, error) {
	var p Product
	err := row.Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.PriceCents,
		&p.Currency, &p.Stock, &p.Reserved, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// CreateProduct inserts a product.
func (s *Store) CreateProduct(ctx context.Context, p Product) (*Product, error) {
	q := `INSERT INTO products (id, sku, name, description, price_cents, currency, stock)
	      VALUES ($1, $2, $3, $4, $5, $6, $7)
	      RETURNING ` + productColumns

	created, err := scanProduct(s.pool.QueryRow(ctx, q,
		p.ID, p.SKU, p.Name, p.Description, p.PriceCents, p.Currency, p.Stock))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return nil, ErrSKUTaken
		}
		return nil, fmt.Errorf("inserting product: %w", err)
	}
	return created, nil
}

// ProductByID returns one product.
func (s *Store) ProductByID(ctx context.Context, id string) (*Product, error) {
	p, err := scanProduct(s.pool.QueryRow(ctx,
		`SELECT `+productColumns+` FROM products WHERE id = $1`, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrProductNotFound
		}
		return nil, fmt.Errorf("querying product: %w", err)
	}
	return p, nil
}

// ListProducts returns a page of products, newest first.
func (s *Store) ListProducts(ctx context.Context, limit, offset int) ([]Product, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+productColumns+` FROM products ORDER BY created_at DESC, id LIMIT $1 OFFSET $2`,
		limit, offset)
	if err != nil {
		return nil, fmt.Errorf("listing products: %w", err)
	}
	defer rows.Close()

	// Non-nil so an empty page marshals as [] rather than null.
	products := make([]Product, 0, limit)
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning product: %w", err)
		}
		products = append(products, *p)
	}
	return products, rows.Err()
}

// Reserve holds quantity of a product for an order.
//
// The whole operation is one UPDATE with the availability check in its WHERE
// clause, so the check and the increment happen atomically inside the database.
// Reading availability into Go and then updating would be a check-then-act
// race: two concurrent orders could both observe enough stock and both reserve
// it, overselling. This is the same class of bug as the rate limiter's
// GET-then-INCR that Phase 2 rules out.
//
// It is idempotent per (order_id, product_id). Phase 5's Kafka delivery is
// at-least-once, so a redelivered OrderCreated must not place a second hold on
// the same stock.
func (s *Store) Reserve(ctx context.Context, orderID, productID string, quantity int) (*Product, error) {
	if quantity <= 0 {
		return nil, fmt.Errorf("quantity must be positive, got %d", quantity)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	// Rollback is a no-op once the transaction has been committed.
	defer func() { _ = tx.Rollback(ctx) }()

	// Record the reservation first. If this order already reserved this
	// product, the insert affects no rows and the hold is not repeated.
	tag, err := tx.Exec(ctx,
		`INSERT INTO reservations (order_id, product_id, quantity)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (order_id, product_id) DO NOTHING`,
		orderID, productID, quantity)
	if err != nil {
		// The foreign key to products fires before any existence check in Go
		// could, so an unknown product surfaces here rather than as ErrNoRows
		// further down.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == foreignKeyViolation {
			return nil, ErrProductNotFound
		}
		return nil, fmt.Errorf("recording reservation: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// Already reserved by this order. Return current state so the caller
		// sees success rather than a spurious failure on redelivery.
		p, err := scanProduct(tx.QueryRow(ctx,
			`SELECT `+productColumns+` FROM products WHERE id = $1`, productID))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrProductNotFound
			}
			return nil, fmt.Errorf("querying product: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("committing: %w", err)
		}
		return p, nil
	}

	// The availability test lives in the WHERE clause, so Postgres evaluates it
	// against the row it is about to lock and update.
	updated, err := scanProduct(tx.QueryRow(ctx,
		`UPDATE products
		 SET reserved = reserved + $2, updated_at = now()
		 WHERE id = $1 AND stock - reserved >= $2
		 RETURNING `+productColumns,
		productID, quantity))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Either the product does not exist or there was not enough
			// available. Distinguish the two for a useful error.
			var exists bool
			if qerr := tx.QueryRow(ctx,
				`SELECT true FROM products WHERE id = $1`, productID).Scan(&exists); qerr != nil {
				if errors.Is(qerr, pgx.ErrNoRows) {
					return nil, ErrProductNotFound
				}
				return nil, fmt.Errorf("checking product existence: %w", qerr)
			}
			return nil, ErrInsufficientStock
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == checkViolation {
			// The reserved_within_stock constraint caught what the WHERE
			// clause should have. Belt and braces.
			return nil, ErrInsufficientStock
		}
		return nil, fmt.Errorf("reserving stock: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing reservation: %w", err)
	}
	return updated, nil
}

// Release returns an order's reservation to available stock.
//
// This is the compensating action for the Phase 5 saga: when payment fails,
// the hold must be given back or that stock is lost until someone notices.
// It is idempotent — releasing a reservation that was already released, or one
// that never existed, succeeds without changing anything, because a
// compensating action may itself be retried.
func (s *Store) Release(ctx context.Context, orderID, productID string) (*Product, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Deleting and returning the quantity in one statement means two concurrent
	// releases cannot both observe the row and both decrement.
	var quantity int
	err = tx.QueryRow(ctx,
		`DELETE FROM reservations WHERE order_id = $1 AND product_id = $2 RETURNING quantity`,
		orderID, productID).Scan(&quantity)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Nothing held. Report current product state rather than an error.
			p, perr := scanProduct(tx.QueryRow(ctx,
				`SELECT `+productColumns+` FROM products WHERE id = $1`, productID))
			if perr != nil {
				if errors.Is(perr, pgx.ErrNoRows) {
					return nil, ErrProductNotFound
				}
				return nil, fmt.Errorf("querying product: %w", perr)
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("committing: %w", err)
			}
			return p, nil
		}
		return nil, fmt.Errorf("deleting reservation: %w", err)
	}

	// GREATEST guards against ever driving reserved negative, which the CHECK
	// constraint would reject and which would abort the compensating action.
	updated, err := scanProduct(tx.QueryRow(ctx,
		`UPDATE products
		 SET reserved = GREATEST(0, reserved - $2), updated_at = now()
		 WHERE id = $1
		 RETURNING `+productColumns,
		productID, quantity))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrProductNotFound
		}
		return nil, fmt.Errorf("releasing stock: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing release: %w", err)
	}
	return updated, nil
}

// Commit converts a reservation into a permanent stock decrement, for when an
// order is confirmed. Both stock and reserved fall by the reserved quantity.
//
// Idempotent for the same reason as Release: the reservation row is deleted in
// the same statement that reads it, so a repeat is a no-op.
func (s *Store) Commit(ctx context.Context, orderID, productID string) (*Product, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var quantity int
	err = tx.QueryRow(ctx,
		`DELETE FROM reservations WHERE order_id = $1 AND product_id = $2 RETURNING quantity`,
		orderID, productID).Scan(&quantity)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			p, perr := scanProduct(tx.QueryRow(ctx,
				`SELECT `+productColumns+` FROM products WHERE id = $1`, productID))
			if perr != nil {
				if errors.Is(perr, pgx.ErrNoRows) {
					return nil, ErrProductNotFound
				}
				return nil, fmt.Errorf("querying product: %w", perr)
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("committing: %w", err)
			}
			return p, nil
		}
		return nil, fmt.Errorf("deleting reservation: %w", err)
	}

	updated, err := scanProduct(tx.QueryRow(ctx,
		`UPDATE products
		 SET stock = GREATEST(0, stock - $2),
		     reserved = GREATEST(0, reserved - $2),
		     updated_at = now()
		 WHERE id = $1
		 RETURNING `+productColumns,
		productID, quantity))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrProductNotFound
		}
		return nil, fmt.Errorf("committing stock: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing stock change: %w", err)
	}
	return updated, nil
}

// AddStock increases physical stock, for restocking.
func (s *Store) AddStock(ctx context.Context, productID string, delta int) (*Product, error) {
	if delta <= 0 {
		return nil, fmt.Errorf("delta must be positive, got %d", delta)
	}
	updated, err := scanProduct(s.pool.QueryRow(ctx,
		`UPDATE products SET stock = stock + $2, updated_at = now()
		 WHERE id = $1 RETURNING `+productColumns,
		productID, delta))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrProductNotFound
		}
		return nil, fmt.Errorf("adding stock: %w", err)
	}
	return updated, nil
}
