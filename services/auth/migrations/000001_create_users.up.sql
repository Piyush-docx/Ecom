-- The auth service owns this table exclusively. No other service reads it;
-- they receive the user's identity from the gateway as a trusted header
-- (IMPLEMENTATION_PLAN.md 1.7).

CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY,
    email         TEXT        NOT NULL,
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Email uniqueness is enforced by the database, not by a check-then-insert in
-- application code: two concurrent signups with the same address would both
-- pass such a check and both insert. The unique index makes the second one
-- fail, which the store translates into ErrEmailTaken.
--
-- The index is on lower(email) so addresses differing only in case collide.
-- Email local-parts are technically case-sensitive per RFC 5321, but no real
-- provider treats them that way, and allowing Alice@x.com alongside
-- alice@x.com invites account-confusion attacks.
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_idx ON users (lower(email));
