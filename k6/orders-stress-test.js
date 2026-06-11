import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { randomIntBetween, randomString } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// ----------------------------------------------------------------------------
// Stress test for the Kafka producer "orders" endpoint.
//
// Target:  POST http://localhost:8080/orders
// Body:    OrderDTO { key, title, description, size }
//
// Run with:
//   k6 run load-tests/orders-stress-test.js
//
// Override the base URL or load profile from the CLI / env, e.g.:
//   k6 run -e BASE_URL=http://localhost:8080 load-tests/orders-stress-test.js
// ----------------------------------------------------------------------------

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const ORDERS_URL = `${BASE_URL}/orders`;

// Custom metrics
const ordersSent = new Counter('orders_sent');
const ordersFailed = new Counter('orders_failed');
const orderLatency = new Trend('order_send_latency', true);

export const options = {
  // Ramp the virtual users up to put progressively more pressure on the broker,
  // hold the peak, then ramp back down.
  scenarios: {
    stress: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 50 },   // warm up
        { duration: '1m', target: 200 },    // ramp to moderate load
        { duration: '2m', target: 500 },    // push toward the limit
        { duration: '1m', target: 800 },    // peak stress
        { duration: '1m', target: 0 },      // recover
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    // Fail the test if more than 1% of requests error out...
    http_req_failed: ['rate<0.01'],
    // ...or if the 95th percentile latency exceeds 1s.
    http_req_duration: ['p(95)<1000'],
  },
};

const TITLES = ['Laptop', 'Phone', 'Headset', 'Keyboard', 'Monitor', 'Mouse', 'Webcam'];

function buildOrder() {
  return JSON.stringify({
    key: `order-${randomString(8)}`,
    title: TITLES[randomIntBetween(0, TITLES.length - 1)],
    description: `Stress test order ${randomString(16)}`,
    size: randomIntBetween(1, 100),
  });
}

export default function () {
  const payload = buildOrder();
  const params = {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /orders' },
  };

  const res = http.post(ORDERS_URL, payload, params);

  orderLatency.add(res.timings.duration);

  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'response received': (r) => r.status !== 0,
  });

  if (ok) {
    ordersSent.add(1);
  } else {
    ordersFailed.add(1);
  }

  // Tiny pause so each VU is not an infinitely tight loop; remove for max RPS.
  sleep(0.1);
}
