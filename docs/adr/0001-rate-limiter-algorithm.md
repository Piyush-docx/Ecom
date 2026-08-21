# ADR 0001: Token bucket as the gateway's default rate-limiting algorithm

- **Status:** Accepted
- **Date:** 2026-08-21
- **Context phase:** Phases 1-3 (implementation), Phase 7 (measurement)

## Context

The gateway must enforce a per-caller request limit shared across every gateway
instance. Three algorithms are implemented in `ratelimiter/`, in-memory
(`algorithms/`) and against Redis (`redis/`):

| Algorithm | State per key | Ceiling |
|---|---|---|
| Token bucket | 2 fields (tokens, timestamp) | soft — bursts to capacity |
| Sliding window counter | 3 fields (window, curr, prev) | approximate |
| Sliding window log | one timestamp per allowed request | exact |

Only one can be the gateway default. `gateway/main.go` picks token bucket; this
records why, and when the other two are the right answer instead.

## Decision

**Token bucket is the default.** The sliding window counter is the alternative
for hard-ceiling cases; the sliding window log is reserved for cases where the
ceiling is contractual.

## Evidence

Both tables below are produced by tests in this repository, not estimated.

### Memory, measured

`ratelimiter/redis/memfootprint_test.go` drives real traffic through the real
Lua scripts against a real Redis and reports `MEMORY USAGE` per key. One key,
limit 10,000/minute:

| Requests admitted | Token bucket | Sliding window counter | Sliding window log |
|---|---|---|---|
| 100 | 176 B | 160 B | 3,704 B |
| 1,000 | 176 B | 160 B | 89,056 B |
| 5,000 | 176 B | 160 B | 499,168 B |

The two O(1) algorithms are flat regardless of traffic. The log grows linearly
at roughly 100 bytes per retained request — about **3,000x** the token bucket
at 5,000 requests, for a *single* key. At a million active keys that is the
difference between ~170 MB and ~500 GB.

This is the decisive constraint. The log is correct but not affordable as a
general-purpose gateway limiter.

### Window-boundary behaviour, measured

`ratelimiter/algorithms/adr_comparison_test.go` drains the limit at the end of
one window, crosses the boundary, and bursts again — measuring what is admitted
in a 3-second span with a limit of 100/minute:

| Algorithm | Admitted in 3s | Note |
|---|---|---|
| Plain fixed window | 100 + 100 = **200** | not implemented here; the flaw being avoided |
| Token bucket | 100 + 3 = **103** | the 3 are refill during the 3s (100/min ≈ 1.67/s) |
| Sliding window counter | 100 + 0 = **100** | |
| Sliding window log | 100 + 0 = **100** | |

A plain fixed-window counter admits **2x the limit** across a boundary. All
three implemented algorithms prevent that. Token bucket's 3 extra requests are
not a boundary artifact — they are the refill rate behaving correctly, and the
same 3 would be admitted at any point in the window.

### The distributed guarantee, measured

Phase 7 test (a) drove 500 requests through nginx across **two** gateway
instances sharing one Redis, limit 100/minute:

```
allowed: 100    denied (429): 400
```

Per-instance limiting would have admitted ~200. Exactly 100 were admitted,
twice, in separate runs. See `deploy/testdata/phase7-acceptance.txt`.

That property comes from the Lua script being atomic, not from the choice of
algorithm — all three share it — but it is the reason any of this holds under
concurrency. An earlier non-atomic GET-then-INCR prototype admitted **1000 of
1000** under the same concurrency.

## Rationale

**Why token bucket by default.** API traffic is bursty. A client that has been
idle then issues ten calls to render one page is behaving normally, and a
limiter that rejects the tenth is measuring the wrong thing. Token bucket
absorbs that burst while still holding the average rate — after the bucket is
spent, the client is throttled to the refill rate. It is also the algorithm
whose *rejection* behaviour is easiest to explain to an API consumer: "you have
N requests banked, refilling at R per second."

**Why not the sliding window counter by default.** It is marginally cheaper
(160 B vs 176 B — noise at this scale) and gives a tighter ceiling, but it has
no burst allowance: a client is throttled to a smooth rate whether or not it
has been idle. For a public API that is a worse experience for well-behaved
clients, and the tighter ceiling buys little when the limit is a
capacity-protection heuristic rather than a contract.

**Why not the sliding window log by default.** The memory table settles it. It
is the only algorithm with no approximation and no burst leniency, which makes
it the right choice when the ceiling is a contractual guarantee — a paid tier
of "exactly 1,000 calls per day", or a limit whose breach costs money
downstream. At those limits n is small and O(n) is affordable. It is the wrong
choice for a general gateway limit of 100/minute per user across a large key
space.

## When to revisit

Switch the gateway to the **sliding window counter** if:

- the limit becomes a published contract where "up to 2x briefly" is not
  acceptable, or
- burst absorption is being abused — a client repeatedly banking a full bucket
  and spending it to spike a downstream service.

Switch a *specific route* to the **sliding window log** if its limit is a hard
guarantee and the key space is small. Nothing in the design prevents different
routes using different limiters; `router.Config` takes a `ratelimiter.Limiter`
and all three satisfy it.

## Consequences

- A client may exceed the nominal average rate briefly, by up to the bucket
  capacity. This is intended, and any downstream service must tolerate a burst
  of `capacity` requests.
- The limiter's state is O(1) per key, so the key space can grow to millions of
  users without a memory plan.
- Rate-limit response headers (`X-RateLimit-Remaining`, `Retry-After`) describe
  a bucket, not a window. `Retry-After` is rounded up to whole seconds by
  `secondsCeil` in `gateway/middleware/ratelimit.go` — that applies to every
  algorithm, not just this one — so the header never advertises a wait a
  fraction shorter than the truth. A client that obeys it exactly is never
  denied twice for the same reason.

## Related

- The limiter runs *after* JWT validation, so authenticated requests are keyed
  by user rather than IP. That ordering has its own tradeoff, noted in
  `gateway/router/router.go`: invalid tokens are rejected without consuming
  quota, which leaves JWT verification itself unprotected by the limiter. HMAC
  verification is cheap enough for that to be a reasonable trade, and it is
  worth revisiting if the gateway is ever exposed to untrusted traffic at scale.
- `/healthz` is deliberately outside the limiter and auth, so a Redis outage
  cannot make every instance look unhealthy and be pulled from rotation.
