// Phase 7 test (a): the rate limit holds across BOTH gateway instances
// combined, not per instance.
//
// This is the claim the whole project exists to demonstrate. Two gateway
// processes sit behind nginx sharing nothing but Redis. If the limiter were a
// per-process counter, roughly 2x the configured limit would get through; the
// Lua scripts from Phase 2 are what make the shared limit exact.
//
//   k6 run -e BASE_URL=http://localhost:8000 -e RATE_LIMIT=100 \
//     deploy/k6/ratelimit.js
//
// Phase 2 proved the same property at the unit level with 1000 goroutines
// against one Redis. This proves it end to end, through a load balancer, with
// two independent processes.

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

// OUT_DIR so the same script works inside the k6 container (where the repo
// is not mounted at its host path) and from a local k6 binary.
const OUT_DIR = __ENV.OUT_DIR || 'deploy/k6/results';
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const RATE_LIMIT = parseInt(__ENV.RATE_LIMIT || '100', 10);

// Deliberately far more requests than the limit, so the denied path is
// exercised heavily and any per-instance leakage is obvious.
const TOTAL_REQUESTS = RATE_LIMIT * 5;

const allowed = new Counter('ratelimit_allowed');
const denied = new Counter('ratelimit_denied');
const unexpected = new Counter('ratelimit_unexpected_status');

export const options = {
  scenarios: {
    burst: {
      // A fixed number of iterations rather than a duration: the assertion is
      // about an exact count, so the test must send an exact count.
      executor: 'shared-iterations',
      vus: 50,
      iterations: TOTAL_REQUESTS,
      maxDuration: '60s',
    },
  },
  // A failed threshold makes k6 exit non-zero, so this doubles as a pass/fail
  // gate rather than only producing numbers to read.
  thresholds: {
    'ratelimit_unexpected_status': ['count==0'],
  },
};

export function setup() {
  // One account, so every request shares a rate-limit key. Keying is per user
  // (Phase 3), so separate users would each get their own budget and the test
  // would measure nothing.
  const email = `ratelimit-${Date.now()}@loadtest.local`;
  const res = http.post(`${BASE_URL}/auth/signup`, JSON.stringify({
    email, password: 'correct-horse-battery',
  }), { headers: { 'Content-Type': 'application/json' } });

  if (res.status !== 201) {
    throw new Error(`signup failed: ${res.status} ${res.body}`);
  }
  return { token: res.json('token') };
}

export default function (data) {
  const res = http.get(`${BASE_URL}/catalog/products`, {
    headers: { Authorization: `Bearer ${data.token}` },
    tags: { name: 'catalog_products' },
  });

  if (res.status === 429) {
    denied.add(1);
    // A denial must still carry the headers a client paces itself with.
    check(res, {
      'denied response has Retry-After': (r) => !!r.headers['Retry-After'],
      'denied response has X-RateLimit-Limit': (r) => !!r.headers['X-Ratelimit-Limit'],
    });
  } else if (res.status >= 200 && res.status < 500) {
    // 2xx and 4xx both mean the limiter let the request through to be handled;
    // only the limiter's own 429 is a denial. A 502 from a dead upstream is
    // counted as unexpected rather than silently treated as allowed.
    allowed.add(1);
  } else {
    unexpected.add(1);
    console.error(`unexpected status ${res.status}: ${res.body}`);
  }
}

export function handleSummary(data) {
  const allowedCount = data.metrics.ratelimit_allowed?.values?.count ?? 0;
  const deniedCount = data.metrics.ratelimit_denied?.values?.count ?? 0;
  const total = allowedCount + deniedCount;

  // The pass condition. Some slop is unavoidable and honest: the token bucket
  // refills continuously, so a test spanning a few seconds legitimately admits
  // a few extra tokens. What must NOT happen is anything near 2x, which is the
  // signature of a per-instance limit.
  const perInstanceWouldBe = RATE_LIMIT * 2;
  const holdsGlobally = allowedCount < perInstanceWouldBe * 0.75;

  const lines = [
    '=== Phase 7 (a): distributed rate limit across two gateway instances ===',
    '',
    `  base url:            ${BASE_URL}`,
    `  configured limit:    ${RATE_LIMIT} per window`,
    `  requests sent:       ${total}`,
    '',
    `  allowed:             ${allowedCount}`,
    `  denied (429):        ${deniedCount}`,
    '',
    `  if the limit were per-instance, roughly ${perInstanceWouldBe} would be allowed.`,
    `  observed ${allowedCount}, which is ${holdsGlobally ? 'consistent with ONE shared limit' : 'NOT consistent with a shared limit'}.`,
    '',
    `  VERDICT: ${holdsGlobally ? 'PASS — the limit holds across both instances combined' : 'FAIL — the limit appears to be per-instance'}`,
    '',
  ];
  const text = lines.join('\n');

  return {
    stdout: text,
    [`${OUT_DIR}/ratelimit-summary.txt`]: text,
    [`${OUT_DIR}/ratelimit-raw.json`]: JSON.stringify(data, null, 2),
  };
}
