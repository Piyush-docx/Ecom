// Phase 7 test (b): a realistic checkout flow at increasing concurrency, to
// find throughput and p50/p95/p99 latency.
//
//   k6 run -e BASE_URL=http://localhost:8000 deploy/k6/checkout.js
//
// These are the numbers that fill in the resume bullet. IMPLEMENTATION_PLAN.md
// §2.6 is explicit that they are measured, never estimated.
//
// The rate limit is set high for this run (see RATE_LIMIT in the compose file):
// the goal here is to find where the SYSTEM saturates, and a limiter throttling
// at 100/min would measure the limiter rather than the stack behind it.

import http from 'k6/http';
import { check, group } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';

// OUT_DIR so the same script works inside the k6 container (where the repo
// is not mounted at its host path) and from a local k6 binary.
const OUT_DIR = __ENV.OUT_DIR || 'deploy/k6/results';
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// Per-step latency, so a slow checkout can be attributed rather than guessed
// at. Ordering is the expensive step -- it prices from the catalog and reserves
// stock synchronously before the saga takes over.
const browseLatency = new Trend('step_browse_duration', true);
const orderLatency = new Trend('step_order_duration', true);
const checkoutLatency = new Trend('checkout_total_duration', true);

const ordersCreated = new Counter('orders_created');
const ordersRateLimited = new Counter('orders_rate_limited');
const checkoutFailures = new Counter('checkout_failures');
const checkoutSuccess = new Rate('checkout_success_rate');

export const options = {
  scenarios: {
    // Ramping arrival rate rather than ramping VUs: this holds the REQUEST rate
    // steady at each step regardless of how slow responses get. With ramping
    // VUs, a system slowing under load would send fewer requests and hide its
    // own degradation.
    checkout: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 500,
      stages: [
        { target: 25, duration: '30s' },
        { target: 50, duration: '30s' },
        { target: 100, duration: '30s' },
        { target: 200, duration: '30s' },
        { target: 200, duration: '30s' }, // hold, to see if it is sustainable
      ],
    },
  },
  // k6's default trend stats omit p(99); the summary reports it, so it must
  // be requested explicitly or it comes back undefined.
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Recorded rather than enforced as a hard gate: the point of this run is to
    // discover the numbers, and a threshold that fails the run would stop it
    // before the higher stages produce data.
    'checkout_total_duration': ['p(95)<3000', 'p(99)<5000'],
    'http_req_failed': ['rate<0.10'],
  },
};

export function setup() {
  // One shared account and one well-stocked product. Creating a product per VU
  // would measure catalog writes rather than the checkout path.
  const email = `checkout-${Date.now()}@loadtest.local`;
  const signup = http.post(`${BASE_URL}/auth/signup`, JSON.stringify({
    email, password: 'correct-horse-battery',
  }), { headers: { 'Content-Type': 'application/json' } });

  if (signup.status !== 201) {
    throw new Error(`signup failed: ${signup.status} ${signup.body}`);
  }
  const token = signup.json('token');
  const authHeaders = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`,
  };

  // Stock far exceeding the run's order count, so the test measures throughput
  // rather than how quickly inventory runs out.
  const product = http.post(`${BASE_URL}/catalog/products`, JSON.stringify({
    sku: `LOADTEST-${Date.now()}`,
    name: 'Load Test Widget',
    price_cents: 2500,
    stock: 1000000,
  }), { headers: authHeaders });

  if (product.status !== 201) {
    throw new Error(`product creation failed: ${product.status} ${product.body}`);
  }

  return { token, productId: product.json('id') };
}

export default function (data) {
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${data.token}`,
  };

  const started = Date.now();
  let ok = true;

  group('browse', function () {
    const res = http.get(`${BASE_URL}/catalog/products/${data.productId}`, {
      headers, tags: { name: 'get_product' },
    });
    browseLatency.add(res.timings.duration);
    if (!check(res, { 'product loaded': (r) => r.status === 200 })) {
      ok = false;
    }
  });

  group('order', function () {
    const res = http.post(`${BASE_URL}/orders/`, JSON.stringify({
      items: [{ product_id: data.productId, quantity: 1 }],
    }), { headers, tags: { name: 'create_order' } });

    orderLatency.add(res.timings.duration);

    if (res.status === 201) {
      ordersCreated.add(1);
    } else if (res.status === 429) {
      // Being throttled is correct behaviour, not a failure. Counted separately
      // so it does not distort the error rate.
      ordersRateLimited.add(1);
    } else {
      ok = false;
      checkoutFailures.add(1);
    }
  });

  checkoutLatency.add(Date.now() - started);
  checkoutSuccess.add(ok);
}

export function handleSummary(data) {
  const m = data.metrics;
  const n = (path, d = 0) => path ?? d;
  const ms = (v) => (v === undefined ? 'n/a' : `${v.toFixed(1)}ms`);

  const created = n(m.orders_created?.values?.count);
  const limited = n(m.orders_rate_limited?.values?.count);
  const failures = n(m.checkout_failures?.values?.count);
  const reqRate = n(m.http_reqs?.values?.rate);
  const total = n(m.checkout_total_duration?.values);

  const lines = [
    '=== Phase 7 (b): checkout flow under increasing concurrency ===',
    '',
    `  base url:              ${BASE_URL}`,
    `  duration:              ${(n(data.state?.testRunDurationMs) / 1000).toFixed(1)}s`,
    '',
    '  THROUGHPUT',
    `    http requests/sec:   ${reqRate.toFixed(1)}`,
    `    orders created:      ${created}`,
    `    orders rate-limited: ${limited}`,
    `    checkout failures:   ${failures}`,
    '',
    '  CHECKOUT LATENCY (browse + order, end to end)',
    `    p50:                 ${ms(total?.['p(50)'] ?? total?.med)}`,
    `    p95:                 ${ms(total?.['p(95)'])}`,
    `    p99:                 ${ms(total?.['p(99)'])}`,
    `    max:                 ${ms(total?.max)}`,
    '',
    '  PER-STEP LATENCY (p95)',
    `    browse product:      ${ms(m.step_browse_duration?.values?.['p(95)'])}`,
    `    create order:        ${ms(m.step_order_duration?.values?.['p(95)'])}`,
    '',
    '  HTTP',
    `    failed rate:         ${(n(m.http_req_failed?.values?.rate) * 100).toFixed(2)}%`,
    `    p95 request:         ${ms(m.http_req_duration?.values?.['p(95)'])}`,
    '',
    '  Note: create_order is synchronous through pricing and stock reservation;',
    '  the payment saga completes asynchronously after the 201 is returned, so',
    '  it is deliberately not inside this latency figure.',
    '',
  ];
  const text = lines.join('\n');

  return {
    stdout: text,
    [`${OUT_DIR}/checkout-summary.txt`]: text,
    [`${OUT_DIR}/checkout-raw.json`]: JSON.stringify(data, null, 2),
  };
}
