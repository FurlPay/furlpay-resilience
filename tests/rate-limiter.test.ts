import test from "node:test";
import assert from "node:assert/strict";
import { TokenBucketLimiter, SlidingWindowLimiter } from "../src/rate-limiter.js";
import { RateLimitExceededError } from "../src/errors.js";
import { ManualClock } from "../src/clock.js";

test("token bucket allows a burst up to capacity, then refuses", () => {
  const clock = new ManualClock();
  const rl = new TokenBucketLimiter({ capacity: 3, refillPerSecond: 1, clock });
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), false, "capacity is the burst ceiling");
});

test("token bucket refills over time and never exceeds capacity", async () => {
  const clock = new ManualClock();
  const rl = new TokenBucketLimiter({ capacity: 2, refillPerSecond: 2, clock });
  rl.tryAcquire();
  rl.tryAcquire();
  assert.equal(rl.tryAcquire(), false);

  await clock.advance(500); // 0.5s at 2/s = 1 token
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), false);

  await clock.advance(60_000); // a long idle must not bank unlimited tokens
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), false, "refill is capped at capacity");
});

test("availableIn reports the real wait", () => {
  const clock = new ManualClock();
  const rl = new TokenBucketLimiter({ capacity: 1, refillPerSecond: 1, clock });
  rl.tryAcquire();
  assert.equal(rl.availableIn(), 1000);
  assert.equal(new TokenBucketLimiter({ capacity: 1, refillPerSecond: 1, clock }).availableIn(), 0);
});

test("acquire waits rather than failing", async () => {
  const clock = new ManualClock();
  const rl = new TokenBucketLimiter({ capacity: 1, refillPerSecond: 10, clock });
  rl.tryAcquire();

  let done = false;
  const p = rl.acquire().then(() => { done = true; });
  await Promise.resolve();
  assert.equal(done, false, "must be waiting");

  await clock.advance(100); // 0.1s at 10/s = 1 token
  await p;
  assert.equal(done, true);
});

test("a cost above capacity is rejected outright, not awaited forever", () => {
  const clock = new ManualClock();
  const rl = new TokenBucketLimiter({ capacity: 5, refillPerSecond: 1, clock });
  assert.throws(() => rl.tryAcquire(6), RangeError);
});

test("sliding window enforces the limit strictly across a boundary", async () => {
  const clock = new ManualClock(0);
  const rl = new SlidingWindowLimiter({ limit: 3, windowMs: 1000, clock });

  // Three at the very end of a notional fixed window...
  await clock.advance(900);
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), true);
  assert.equal(rl.tryAcquire(), true);

  // ...and three more just after it. A FIXED window would allow all six,
  // which is the burst that gets an API key suspended.
  await clock.advance(200);
  assert.equal(rl.tryAcquire(), false, "a rolling window must not permit 2x at the boundary");
});

test("sliding window frees capacity as entries age out", async () => {
  const clock = new ManualClock(0);
  const rl = new SlidingWindowLimiter({ limit: 2, windowMs: 1000, clock });
  rl.tryAcquire();
  rl.tryAcquire();
  assert.equal(rl.tryAcquire(), false);

  await clock.advance(1001);
  assert.equal(rl.tryAcquire(), true, "the oldest entries have left the window");
});

test("acquireOrThrow fails fast with the wait time", () => {
  const clock = new ManualClock(0);
  const rl = new SlidingWindowLimiter({ limit: 1, windowMs: 5_000, clock });
  rl.acquireOrThrow();

  const err = (() => { try { rl.acquireOrThrow(); } catch (e) { return e; } })();
  assert.ok(err instanceof RateLimitExceededError);
  const rle = err as RateLimitExceededError;
  assert.equal(rle.limit, 1);
  assert.equal(rle.windowMs, 5_000);
  assert.ok(rle.retryAfterMs > 0 && rle.retryAfterMs <= 5_000);
  assert.equal(rle.retryable, true, "a rate limit is retryable by definition");
});

test("rejects nonsensical configuration instead of misbehaving quietly", () => {
  assert.throws(() => new TokenBucketLimiter({ capacity: 0, refillPerSecond: 1 }), RangeError);
  assert.throws(() => new TokenBucketLimiter({ capacity: 1, refillPerSecond: 0 }), RangeError);
  assert.throws(() => new SlidingWindowLimiter({ limit: 0, windowMs: 1000 }), RangeError);
  assert.throws(() => new SlidingWindowLimiter({ limit: 1, windowMs: 0 }), RangeError);
});
