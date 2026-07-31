-- One database per service, per IMPLEMENTATION_PLAN.md 1.3: "no shared tables,
-- no cross-service joins".
--
-- Separate databases rather than separate schemas in one database, because a
-- schema boundary is only a convention — a service could still join across it.
-- With separate databases each service physically cannot reach another's
-- tables, which is the isolation the plan is asking for.
--
-- This runs once, on first initialization of the data volume. Changing it later
-- requires `docker compose down -v` to take effect.

CREATE DATABASE ecom_auth;
CREATE DATABASE ecom_catalog;
CREATE DATABASE ecom_orders;
CREATE DATABASE ecom_payment;
