package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"pkg/correlation"
)

var (
	// ErrCatalogNotFound means the catalog reported a 404.
	ErrCatalogNotFound = errors.New("product not found in catalog")

	// ErrCatalogConflict means the catalog refused for a state reason,
	// in practice insufficient stock.
	ErrCatalogConflict = errors.New("catalog rejected the request")
)

// CatalogClient calls the catalog service.
//
// This is a synchronous dependency, which Phase 5 partly replaces with events.
// It stays for pricing, which needs an answer before an order can be recorded.
type CatalogClient struct {
	baseURL string
	http    *http.Client
}

// NewCatalogClient returns a client for the catalog at baseURL.
func NewCatalogClient(baseURL string) *CatalogClient {
	return &CatalogClient{
		baseURL: baseURL,
		http: &http.Client{
			// A bounded timeout keeps a slow catalog from holding orders'
			// request goroutines open indefinitely.
			Timeout: 10 * time.Second,
		},
	}
}

// CatalogProduct is the subset of a product this service needs.
type CatalogProduct struct {
	ID         string `json:"id"`
	PriceCents int64  `json:"price_cents"`
	Currency   string `json:"currency"`
	Available  int    `json:"available"`
}

// Product fetches one product for pricing.
func (c *CatalogClient) Product(ctx context.Context, productID string) (*CatalogProduct, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/catalog/products/"+productID, nil)
	if err != nil {
		return nil, fmt.Errorf("building catalog request: %w", err)
	}
	// Carry the trace across the service boundary, so Phase 6 can follow one
	// request through both services.
	correlation.SetHeader(ctx, req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling catalog: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, ErrCatalogNotFound
	default:
		return nil, fmt.Errorf("catalog returned %d", resp.StatusCode)
	}

	var p CatalogProduct
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, fmt.Errorf("decoding catalog response: %w", err)
	}
	return &p, nil
}

// Reserve asks the catalog to hold stock for an order.
func (c *CatalogClient) Reserve(ctx context.Context, orderID, productID string, quantity int) error {
	return c.reservationCall(ctx, "/catalog/reservations", map[string]any{
		"order_id": orderID, "product_id": productID, "quantity": quantity,
	})
}

// Release returns a hold to available stock. This is the compensating action
// for a failed order.
func (c *CatalogClient) Release(ctx context.Context, orderID, productID string) error {
	return c.reservationCall(ctx, "/catalog/reservations/release", map[string]any{
		"order_id": orderID, "product_id": productID,
	})
}

// Commit converts a hold into a permanent stock decrement.
func (c *CatalogClient) Commit(ctx context.Context, orderID, productID string) error {
	return c.reservationCall(ctx, "/catalog/reservations/commit", map[string]any{
		"order_id": orderID, "product_id": productID,
	})
}

func (c *CatalogClient) reservationCall(ctx context.Context, path string, body map[string]any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encoding catalog request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("building catalog request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	correlation.SetHeader(ctx, req)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling catalog: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return ErrCatalogNotFound
	case http.StatusConflict:
		return ErrCatalogConflict
	default:
		return fmt.Errorf("catalog returned %d", resp.StatusCode)
	}
}
