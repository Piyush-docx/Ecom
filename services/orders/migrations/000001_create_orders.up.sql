-- The orders service owns orders and their line items. It never reads the
-- catalog's or payment's tables; it learns about them through events (Phase 5)
-- or synchronous calls (Phase 4).

CREATE TABLE IF NOT EXISTS orders (
    id         UUID PRIMARY KEY,
    user_id    TEXT        NOT NULL,

    -- The saga's state machine. An order starts pending, then either becomes
    -- confirmed (payment succeeded) or cancelled (payment failed, and the
    -- inventory reservation has been compensated).
    --
    -- Enforced by the database rather than only in Go: a typo'd status written
    -- by any future code path would otherwise leave an order in a state no
    -- consumer knows how to handle, and it would be found in production.
    status     TEXT        NOT NULL CHECK (status IN ('pending', 'confirmed', 'cancelled')),

    -- Denormalized total in minor units, fixed at creation. Prices change; an
    -- order must remember what was actually charged.
    total_cents BIGINT     NOT NULL CHECK (total_cents >= 0),
    currency   TEXT        NOT NULL DEFAULT 'USD',

    -- Set when an order reaches a terminal state, for debugging why.
    failure_reason TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_user_idx ON orders (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status);

CREATE TABLE IF NOT EXISTS order_items (
    order_id    UUID    NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id  UUID    NOT NULL,
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    -- Unit price at the time of ordering, not the current catalog price.
    unit_cents  BIGINT  NOT NULL CHECK (unit_cents >= 0),
    PRIMARY KEY (order_id, product_id)
);
