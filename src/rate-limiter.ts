import { RateLimitExceededError } from "./errors.js";
import { systemClock, type Clock } from "./clock.js";

// ---------------------------------------------------------------------------
// Client-side rate limiting.
//
// This is the OUTBOUND limiter — it stops FurlPay exceeding a provider's quota,
// which is a different job from the inbound limiter that protects FurlPay's own
// routes. Being 429'd by a market-data provider is self-inflicted and cheap to
// avoid: shape the traffic before it leaves.
//
// Two algorithms, because they answer different questions:
//
//   TOKEN BUCKET  — allows a burst up to the bucket size, then settles to the
//                   refill rate. Right for providers that publish a sustained
//                   rate and tolerate spikes. Constant memory.
//
//   SLIDING WINDOW — enforces "no more than N in any rolling T", strictly. Right
//                   for a hard published quota ("100 requests per minute") where
//                   a burst would breach it. Stores a timestamp per request, so
//                   memory is O(limit).
//
// A fixed window is deliberately not offered: it permits 2x the limit across a
// boundary (N at 59.9s, N more at 60.1s), which is exactly the burst that gets
// an API key suspended.
// ---------------------------------------------------------------------------

export interface RateLimiter {
  /** Consume `cost`, waiting if necessary. Rejects if `signal` aborts. */
  acquire(cost?: number, signal?: AbortSignal): Promise<void>;
  /** Consume without waiting. False when it would exceed the limit. */
  tryAcquire(cost?: number): boolean;
  /** Milliseconds until `cost` would be available. 0 when available now. */
  availableIn(cost?: number): number;
}

export interface TokenBucketOptions {
  /** Bucket capacity — the largest burst permitted. */
  capacity: number;
  /** Tokens added per second. */
  refillPerSecond: number;
  /** Start full (default) or empty. */
  startFull?: boolean;
  clock?: Clock;
}

export class TokenBucketLimiter implements RateLimiter {
  private readonly capacity: number;
  private readonly refillPerSecond: number;
  private readonly clock: Clock;
  private tokens: number;
  private lastRefill: number;

  constructor(opts: TokenBucketOptions) {
    if (opts.capacity <= 0) throw new RangeError("capacity must be > 0");
    if (opts.refillPerSecond <= 0) throw new RangeError("refillPerSecond must be > 0");
    this.capacity = opts.capacity;
    this.refillPerSecond = opts.refillPerSecond;
    this.clock = opts.clock ?? systemClock;
    this.tokens = opts.startFull === false ? 0 : opts.capacity;
    this.lastRefill = this.clock.now();
  }

  private refill(): void {
    const now = this.clock.now();
    const elapsedMs = now - this.lastRefill;
    if (elapsedMs <= 0) return;
    this.tokens = Math.min(this.capacity, this.tokens + (elapsedMs / 1000) * this.refillPerSecond);
    this.lastRefill = now;
  }

  tryAcquire(cost = 1): boolean {
    if (cost > this.capacity) {
      // Would never succeed at any wait. Say so now rather than block forever.
      throw new RangeError(`cost ${cost} exceeds bucket capacity ${this.capacity}`);
    }
    this.refill();
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return true;
    }
    return false;
  }

  availableIn(cost = 1): number {
    this.refill();
    if (this.tokens >= cost) return 0;
    return Math.ceil(((cost - this.tokens) / this.refillPerSecond) * 1000);
  }

  async acquire(cost = 1, signal?: AbortSignal): Promise<void> {
    for (;;) {
      signal?.throwIfAborted();
      if (this.tryAcquire(cost)) return;
      const waitMs = this.availableIn(cost);
      // Loop rather than trusting one sleep: another caller may take the tokens
      // while this one waits, and a single sleep-then-consume would let it
      // proceed over the limit.
      await this.clock.sleep(Math.max(1, waitMs), signal);
    }
  }
}

export interface SlidingWindowOptions {
  /** Maximum requests in any rolling window. */
  limit: number;
  windowMs: number;
  clock?: Clock;
}

export class SlidingWindowLimiter implements RateLimiter {
  private readonly limit: number;
  private readonly windowMs: number;
  private readonly clock: Clock;
  private hits: number[] = [];

  constructor(opts: SlidingWindowOptions) {
    if (opts.limit <= 0) throw new RangeError("limit must be > 0");
    if (opts.windowMs <= 0) throw new RangeError("windowMs must be > 0");
    this.limit = opts.limit;
    this.windowMs = opts.windowMs;
    this.clock = opts.clock ?? systemClock;
  }

  private evict(now: number): void {
    const cutoff = now - this.windowMs;
    // Timestamps are appended in order, so the expired ones are a prefix.
    let i = 0;
    while (i < this.hits.length && this.hits[i] <= cutoff) i++;
    if (i > 0) this.hits = this.hits.slice(i);
  }

  tryAcquire(cost = 1): boolean {
    if (cost > this.limit) throw new RangeError(`cost ${cost} exceeds limit ${this.limit}`);
    const now = this.clock.now();
    this.evict(now);
    if (this.hits.length + cost > this.limit) return false;
    for (let i = 0; i < cost; i++) this.hits.push(now);
    return true;
  }

  availableIn(cost = 1): number {
    const now = this.clock.now();
    this.evict(now);
    if (this.hits.length + cost <= this.limit) return 0;
    // Wait until enough of the oldest entries fall out of the window.
    const needed = this.hits.length + cost - this.limit;
    const freeingAt = this.hits[needed - 1];
    return Math.max(1, freeingAt + this.windowMs - now);
  }

  async acquire(cost = 1, signal?: AbortSignal): Promise<void> {
    for (;;) {
      signal?.throwIfAborted();
      if (this.tryAcquire(cost)) return;
      await this.clock.sleep(this.availableIn(cost), signal);
    }
  }

  /** Reject immediately instead of waiting — for a path where queueing is worse
   *  than failing, such as a request already near its own deadline. */
  acquireOrThrow(cost = 1): void {
    if (!this.tryAcquire(cost)) {
      throw new RateLimitExceededError(this.availableIn(cost), this.limit, this.windowMs);
    }
  }
}
