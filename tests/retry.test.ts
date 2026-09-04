import test from "node:test";
import assert from "node:assert/strict";
import {
  executeWithRetry, backoffDelay, parseRetryAfter, isRetryableError, isMethodRetryable,
} from "../src/retry.js";
import { MaxRetriesExceededError } from "../src/errors.js";
import { ManualClock } from "../src/clock.js";

const status = (n: number, headers?: Record<string, string>) =>
  Object.assign(new Error(`HTTP ${n}`), { status: n, headers });

test("full jitter samples the whole interval, and the ceiling doubles", () => {
  // random() = 1 would be out of range; 0.999… approximates the top.
  assert.equal(backoffDelay(0, 100, 10_000, () => 0.99999), 99);
  assert.equal(backoffDelay(1, 100, 10_000, () => 0.99999), 199);
  assert.equal(backoffDelay(2, 100, 10_000, () => 0.99999), 399);
  assert.equal(backoffDelay(3, 100, 10_000, () => 0.99999), 799);
  // Full jitter must be able to return 0 — that is what decorrelates callers.
  assert.equal(backoffDelay(5, 100, 10_000, () => 0), 0);
});

test("backoff is capped by maxDelayMs", () => {
  assert.equal(backoffDelay(20, 100, 5_000, () => 0.99999), 4_999);
});

test("parses Retry-After in both RFC 9110 forms", () => {
  const now = Date.parse("2026-01-01T00:00:00Z");
  assert.equal(parseRetryAfter("120", now), 120_000, "delay-seconds");
  assert.equal(parseRetryAfter("Thu, 01 Jan 2026 00:02:00 GMT", now), 120_000, "HTTP-date");
  // A date already past means retry now, never a negative delay.
  assert.equal(parseRetryAfter("Thu, 01 Jan 2026 00:00:00 GMT", now + 5_000), 0);
  assert.equal(parseRetryAfter(undefined, now), undefined);
  assert.equal(parseRetryAfter("not-a-date", now), undefined);
  assert.equal(parseRetryAfter("  ", now), undefined);
});

test("client errors are never retried", () => {
  for (const s of [400, 401, 403, 404, 422]) {
    assert.equal(isRetryableError(status(s)), false, `${s} must not retry`);
  }
});

test("server and throttling errors are retried", () => {
  for (const s of [408, 429, 500, 502, 503, 504]) {
    assert.equal(isRetryableError(status(s)), true, `${s} should retry`);
  }
});

test("an unknown status falls back on the 4xx/5xx split", () => {
  assert.equal(isRetryableError(status(418)), false, "unknown 4xx is a client problem");
  assert.equal(isRetryableError(status(599)), true, "unknown 5xx is a server problem");
});

test("a transport error with no status is retried", () => {
  assert.equal(isRetryableError(new Error("ECONNRESET")), true);
});

test("an abort is a decision, not a fault", () => {
  const e = new Error("Aborted");
  e.name = "AbortError";
  assert.equal(isRetryableError(e), false);
});

test("unsafe methods need an idempotency key", () => {
  for (const m of ["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]) {
    assert.equal(isMethodRetryable({ method: m }), true, `${m} is idempotent`);
  }
  // Retrying a POST whose response was lost can charge twice.
  assert.equal(isMethodRetryable({ method: "POST" }), false);
  assert.equal(isMethodRetryable({ method: "PATCH" }), false);
  assert.equal(isMethodRetryable({ method: "POST", hasIdempotencyKey: true }), true);
  assert.equal(isMethodRetryable({ method: "POST", allowUnsafeMethodRetry: true }), true);
});

test("a 404 fails on the first attempt with the original error", async () => {
  const clock = new ManualClock();
  let calls = 0;
  const err = await executeWithRetry(async () => { calls++; throw status(404); },
    { clock, random: () => 0, maxRetries: 5 }).catch((e) => e);

  assert.equal(calls, 1, "must not retry a 404");
  assert.equal((err as { status: number }).status, 404, "the caller sees the 404, not a wrapper");
});

test("retries a 503 and succeeds", async () => {
  const clock = new ManualClock();
  let calls = 0;
  const p = executeWithRetry(async () => {
    calls++;
    if (calls < 3) throw status(503);
    return "recovered";
  }, { clock, random: () => 0, baseDelayMs: 100, maxRetries: 5 });

  await clock.advance(1000);
  assert.equal(await p, "recovered");
  assert.equal(calls, 3);
});

test("exhausting retries yields MaxRetriesExceededError with the attempt count", async () => {
  const clock = new ManualClock();
  let calls = 0;
  const p = executeWithRetry(async () => { calls++; throw status(500); },
    { clock, random: () => 0, maxRetries: 2 }).catch((e) => e);

  await clock.advance(10_000);
  const err = await p;
  assert.ok(err instanceof MaxRetriesExceededError);
  assert.equal(err.attempts, 3, "1 initial + 2 retries");
  assert.equal(calls, 3);
});

test("Retry-After overrides the computed backoff", async () => {
  const clock = new ManualClock();
  const delays: number[] = [];
  let calls = 0;

  const p = executeWithRetry(async () => {
    calls++;
    if (calls === 1) throw status(429, { "retry-after": "5" });
    return "ok";
  }, {
    clock,
    // Jitter would have produced ~0; the server's 5s must win.
    random: () => 0,
    baseDelayMs: 100,
    onRetry: ({ delayMs }) => delays.push(delayMs),
  });

  await clock.advance(5_000);
  assert.equal(await p, "ok");
  assert.deepEqual(delays, [5_000]);
});

test("an absurd Retry-After is capped", async () => {
  const clock = new ManualClock();
  const delays: number[] = [];
  let calls = 0;

  const p = executeWithRetry(async () => {
    calls++;
    if (calls === 1) throw status(503, { "retry-after": "86400" }); // a day
    return "ok";
  }, { clock, random: () => 0, maxRetryAfterMs: 30_000, onRetry: ({ delayMs }) => delays.push(delayMs) });

  await clock.advance(30_000);
  assert.equal(await p, "ok");
  assert.deepEqual(delays, [30_000], "an upstream cannot hang the caller for a day");
});

test("a POST without an idempotency key is not retried", async () => {
  const clock = new ManualClock();
  let calls = 0;
  await executeWithRetry(async () => { calls++; throw status(503); },
    { clock, method: "POST", maxRetries: 5, random: () => 0 }).catch(() => {});
  assert.equal(calls, 1, "a lost ACK is not proof the charge failed");
});

test("the same POST WITH an idempotency key is retried", async () => {
  const clock = new ManualClock();
  let calls = 0;
  const p = executeWithRetry(async () => {
    calls++;
    if (calls < 2) throw status(503);
    return "ok";
  }, { clock, method: "POST", hasIdempotencyKey: true, maxRetries: 5, random: () => 0 });

  await clock.advance(1_000);
  assert.equal(await p, "ok");
  assert.equal(calls, 2);
});

test("an aborted signal stops the ladder", async () => {
  const clock = new ManualClock();
  const ac = new AbortController();
  let calls = 0;

  const p = executeWithRetry(async () => { calls++; throw status(503); },
    { clock, signal: ac.signal, maxRetries: 5, baseDelayMs: 100, random: () => 0.5 }).catch((e) => e);

  await Promise.resolve();
  ac.abort();
  await clock.advance(10_000);
  await p;
  assert.ok(calls <= 2, `aborted mid-backoff should stop quickly, saw ${calls} calls`);
});
