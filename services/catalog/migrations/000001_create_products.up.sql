-- The catalog service owns products and their inventory.

CREATE TABLE IF NOT EXISTS products (
    id          UUID PRIMARY KEY,
    sku         TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    -- Money is stored in minor units (cents) as a bigint, never as a float.
    -- Binary floating point cannot represent 0.10 exactly, so accumulating
    -- prices in a float silently loses money.
    price_cents BIGINT      NOT NULL CHECK (price_cents >= 0),
    currency    TEXT        NOT NULL DEFAULT 'USD',

    -- stock is what physically exists; reserved is the part of it already
    -- promised to pending orders. Available stock is (stock - reserved).
    --
    -- Holding these separately is what makes the Phase 5 saga's compensating
    -- action possible: a failed payment releases the reservation without ever
    -- having decremented real stock.
    stock       INTEGER     NOT NULL DEFAULT 0 CHECK (stock >= 0),
    reserved    INTEGER     NOT NULL DEFAULT 0 CHECK (reserved >= 0),

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The database refuses to promise more than exists. Application code can
    -- be wrong; this constraint cannot be bypassed by any code path, including
    -- ones written later.
    CONSTRAINT reserved_within_stock CHECK (reserved <= stock)
);

CREATE UNIQUE INDEX IF NOT EXISTS products_sku_idx ON products (sku);

-- Reservations are tracked per order so a compensating release knows exactly
-- how much to give back, and so a redelivered event cannot double-reserve.
--
-- Phase 5 relies on this: Kafka is at-least-once, so the same OrderCreated may
-- arrive twice. The primary key makes the second reservation a no-op rather
-- than a second hold on the same stock.
CREATE TABLE IF NOT EXISTS reservations (
    order_id   UUID        NOT NULL,
    product_id UUID        NOT NULL REFERENCES products (id),
    quantity   INTEGER     NOT NULL CHECK (quantity > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS reservations_order_idx ON reservations (order_id);
