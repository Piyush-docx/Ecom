package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Errors the store returns for conditions the HTTP layer must distinguish.
var (
	// ErrEmailTaken means the unique index rejected the insert.
	ErrEmailTaken = errors.New("email already registered")

	// ErrUserNotFound means no row matched.
	ErrUserNotFound = errors.New("user not found")
)

// User is a row of the users table.
type User struct {
	ID           string
	Email        string
	PasswordHash string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Store is the auth service's data access layer.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// uniqueViolation is Postgres's SQLSTATE for a unique constraint failure.
const uniqueViolation = "23505"

// CreateUser inserts a new user.
//
// It relies on the database's unique index rather than checking for an existing
// row first. A SELECT-then-INSERT would let two concurrent signups for the same
// address both observe "no such user" and both proceed; only one insert can win
// against the index, and the loser is reported as ErrEmailTaken.
func (s *Store) CreateUser(ctx context.Context, id, email, passwordHash string) (*User, error) {
	const q = `
		INSERT INTO users (id, email, password_hash)
		VALUES ($1, $2, $3)
		RETURNING id, email, password_hash, created_at, updated_at`

	var u User
	err := s.pool.QueryRow(ctx, q, id, email, passwordHash).
		Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return nil, ErrEmailTaken
		}
		return nil, fmt.Errorf("inserting user: %w", err)
	}
	return &u, nil
}

// UserByEmail looks up a user by email, case-insensitively to match the unique
// index.
func (s *Store) UserByEmail(ctx context.Context, email string) (*User, error) {
	const q = `
		SELECT id, email, password_hash, created_at, updated_at
		FROM users
		WHERE lower(email) = lower($1)`

	var u User
	err := s.pool.QueryRow(ctx, q, strings.TrimSpace(email)).
		Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("querying user by email: %w", err)
	}
	return &u, nil
}

// UserByID looks up a user by primary key.
func (s *Store) UserByID(ctx context.Context, id string) (*User, error) {
	const q = `
		SELECT id, email, password_hash, created_at, updated_at
		FROM users
		WHERE id = $1`

	var u User
	err := s.pool.QueryRow(ctx, q, id).
		Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("querying user by id: %w", err)
	}
	return &u, nil
}
