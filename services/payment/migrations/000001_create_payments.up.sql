-- The payment service owns charge attempts.

CREATE TABLE IF NOT EXISTS payments (
    id         UUID PRIMARY KEY,

    -- The idempotency key. IMPLEMENTATION_PLAN.md Phase 5 requires that a
    -- redelivered OrderCreated results in exactly one charge attempt, and
    -- Kafka's at-least-once delivery guarantees redelivery will happen.
    --
    -- A unique index is what makes that true. An application-level "have I
    -- seen this order?" check cannot: two concurrent deliveries would both
    -- find nothing and both charge the customer.
    order_id   UUID        NOT NULL,

    user_id    TEXT        NOT NULL,
    amount_cents BIGINT    NOT NULL CHECK (amount_cents >= 0),
    currency   TEXT        NOT NULL DEFAULT 'USD',

    status     TEXT        NOT NULL CHECK (status IN ('succeeded', 'failed')),
    failure_reason TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS payments_order_idx ON payments (order_id);
CREATE INDEX IF NOT EXISTS payments_user_idx ON payments (user_id, created_at DESC);
