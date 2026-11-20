// k6 run -e RATE=100 -e DURATION=2m scripts/load.js
// Biến: RATE (req/s), DURATION, WALLETS (số ví, mặc định 1000), HOT (0..1: tỷ lệ request dồn vào ví W0, để thử hot row/hot key)
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const RATE = Number(__ENV.RATE || 50);
const WALLETS = Number(__ENV.WALLETS || 1000);
const HOT = Number(__ENV.HOT || 0);
const RUN = Date.now();

const completed = new Counter('transfer_completed');
const failed = new Counter('transfer_failed');
const unknown = new Counter('transfer_unknown');

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s',
      duration: __ENV.DURATION || '2m',
      preAllocatedVUs: Math.max(20, RATE), maxVUs: Math.max(200, RATE * 4),
    },
  },
  thresholds: { http_req_failed: ['rate<0.05'], http_req_duration: ['p(95)<1000'] },
};

function pick() { return 'W' + Math.floor(Math.random() * WALLETS); }

export default function () {
  const from = Math.random() < HOT ? 'W0' : pick();
  let to = pick(); if (to === from) to = 'W1';
  const key = `${RUN}-${__VU}-${__ITER}`;   // Idempotency-Key duy nhất mỗi request
  const res = http.post('http://localhost:8082/transfers',
    JSON.stringify({ from, to, amount: 1 }),
    { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key } });
  check(res, { 'http 200': (r) => r.status === 200 });
  try {
    const st = res.json('status');
    if (st === 'COMPLETED') completed.add(1); else if (st === 'FAILED') failed.add(1); else unknown.add(1);
  } catch (e) { unknown.add(1); }
}
