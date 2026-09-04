# @furlpay/resilience

[![npm](https://img.shields.io/npm/v/%40furlpay%2Fresilience?logo=npm&color=CB3837)](https://www.npmjs.com/package/@furlpay/resilience) ![TypeScript](https://img.shields.io/badge/TypeScript-5.5-3178C6?logo=typescript&logoColor=white) ![Node.js](https://img.shields.io/badge/Node.js-%E2%89%A520-339933?logo=nodedotjs&logoColor=white) ![Edge runtime](https://img.shields.io/badge/Edge_runtime-compatible-000000?logo=vercel&logoColor=white) ![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen) ![License](https://img.shields.io/badge/License-MIT-blue)

Resilience primitives for financial clients. Zero runtime dependencies — native
`fetch`, `AbortController`, `crypto` and standard collections only, so it runs
unchanged on Node 20+ and Edge runtimes.

A payment path should not inherit a supply chain. Every dependency here would be
another party with reach into money movement, so there are none.

## What's in it

| Module | Purpose |
|---|---|
| `CircuitBreaker` | Sliding-window breaker. Slow calls count as failures. |
| `executeWithRetry` | Full-jitter exponential backoff, `Retry-After` aware. |
| `TokenBucketLimiter` / `SlidingWindowLimiter` | Outbound rate limiting. |
| `FailoverPool` | Multi-endpoint RPC/provider failover with per-endpoint breakers. |
| `ResilientHttpClient` | The composition: limit → breaker → retry → timeout → fetch. |
| `ResilientEventSource` | SSE with heartbeat watchdog and `Last-Event-ID` resume. |
| `Clock` / `ManualClock` | Injectable time. Makes every test above deterministic. |

## Why some of it is shaped unusually

**Slow calls trip the breaker.** A provider answering `200 OK` in 30 seconds does
more damage than one returning `503` immediately, because callers keep waiting. A
breaker that only counts errors never trips on the outage that actually hurts.

**Full jitter, not exponential-plus-noise.** When a provider 503s a thousand
callers at once, fixed backoff retries all thousand in the same millisecond.
Sampling uniformly from `[0, ceiling)` decorrelates them; partial jitter still
leaves a spike at the interval's end.

**POST is not retried without an idempotency key.** A retry of a request whose
response you never saw can charge twice. The request may well have succeeded and
only the ACK was lost — silence is not failure.

**The timeout uses a ref'd timer, not `AbortSignal.timeout`.** Node unrefs that
one, so when a request is the only pending work — the ordinary case in a
serverless handler — the timeout never fires and the promise never settles. This
was found by the test suite, not by review.

**Each pool endpoint gets its own breaker.** One shared breaker means the first
bad provider trips it and the healthy ones become unreachable, turning a partial
outage into a total one.

**Nothing starts a background timer.** `FailoverPool.probe()` is called by your
scheduler. A library that spawns intervals keeps a serverless function alive and
billing.

## Usage

```ts
import { ResilientHttpClient, FailoverPool, TokenBucketLimiter } from "@furlpay/resilience";

const rpc = new FailoverPool({
  name: "solana",
  endpoints: [
    { id: "helius", url: "https://rpc-a.example", weight: 2 },
    { id: "quicknode", url: "https://rpc-b.example" },
  ],
  circuitBreaker: { failureRateThreshold: 0.5, slidingWindowSize: 20 },
});

const client = new ResilientHttpClient({
  baseUrl: "https://api.furlpay.com",
  timeoutMs: 5_000,
  rateLimiter: new TokenBucketLimiter({ capacity: 20, refillPerSecond: 10 }),
  circuitBreaker: { failureRateThreshold: 0.5 },
  retry: { maxRetries: 3, baseDelayMs: 200 },
  auth: {
    getAccessToken: () => loadToken(),
    refreshToken: () => refresh(),
  },
  telemetry: { onMetric: (m) => log.info("upstream", m) },
});

// A POST becomes retry-safe only with a key.
await client.post("/payments", { amount: 100 }, { idempotencyKey: crypto.randomUUID() });
```

## Testing

Time is injected, not mocked. Tests drive a `ManualClock` directly, so "wait ten
seconds for the breaker to half-open" is an assignment rather than a sleep:

```ts
const clock = new ManualClock();
const cb = new CircuitBreaker({ waitDurationInOpenStateMs: 10_000, clock });
await clock.advance(10_000);
assert.equal(cb.getState(), "HALF_OPEN");
```

This beats global fake timers: those are process-wide, leak between files when a
restore is missed, and cannot represent two components on different clocks.

```bash
npm test   # 69 tests
```

MIT.
