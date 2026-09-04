import { CircuitOpenError, type CircuitMetrics } from "./errors.js";
import { systemClock, type Clock } from "./clock.js";

// ---------------------------------------------------------------------------
// Sliding-window circuit breaker.
//
// The failure it prevents is not "one call failed" — a retry handles that. It
// is the cascade: a degraded provider takes 30s to time out, every request
// queues behind it, connections exhaust, and a slow dependency becomes a total
// outage of everything that touches it. The breaker fails fast so the rest of
// the system stays up.
//
//   CLOSED     calls pass; outcomes recorded in a fixed-size ring
//              failure rate >= threshold over a FULL window -> OPEN
//   OPEN       calls rejected immediately, downstream untouched
//              after waitDurationInOpenState -> HALF_OPEN
//   HALF_OPEN  a bounded number of probes admitted
//              all succeed -> CLOSED     any fail -> OPEN (timer restarts)
//
// SLOW CALLS COUNT AS FAILURES. A provider answering 200 OK in 30 seconds is
// doing more damage than one returning 503 immediately, because the caller
// keeps waiting. A breaker that only counts errors never trips on the outage
// that actually hurts.
//
// The window must be FULL before the rate can trip it. Without that, the very
// first failed call is a 100% failure rate and the breaker opens on a single
// blip.
// ---------------------------------------------------------------------------

export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export interface CircuitBreakerOptions {
  name?: string;
  /** Trip when this fraction of the window failed. 0..1, default 0.5. */
  failureRateThreshold?: number;
  /** Trip when this fraction of the window was slow. 0..1, default 1 (off). */
  slowCallRateThreshold?: number;
  /** A call at or beyond this duration counts as slow. Default 2000ms. */
  slowCallDurationThresholdMs?: number;
  /** Ring size. Default 20. */
  slidingWindowSize?: number;
  /** Minimum recorded calls before any rate can trip. Defaults to the window size. */
  minimumNumberOfCalls?: number;
  /** How long OPEN lasts before a probe is admitted. Default 10000ms. */
  waitDurationInOpenStateMs?: number;
  /** Probes admitted in HALF_OPEN. Default 3. */
  permittedNumberOfCallsInHalfOpenState?: number;
  /** Return false to record an error as a SUCCESS — e.g. a 404, which proves
   *  the provider is healthy and the resource is absent. Counting it as a
   *  failure trips the breaker on ordinary application errors. */
  recordFailurePredicate?: (error: unknown) => boolean;
  clock?: Clock;
}

type Outcome = "success" | "failure" | "slow";

export class CircuitBreaker {
  readonly name: string;
  private readonly failureRateThreshold: number;
  private readonly slowCallRateThreshold: number;
  private readonly slowCallDurationThresholdMs: number;
  private readonly windowSize: number;
  private readonly minimumCalls: number;
  private readonly openDurationMs: number;
  private readonly halfOpenPermitted: number;
  private readonly recordFailure: (error: unknown) => boolean;
  private readonly clock: Clock;

  private state: CircuitState = "CLOSED";
  private ring: Outcome[] = [];
  private cursor = 0;
  private openedAt = 0;
  /** Probes admitted so far in the current HALF_OPEN period. */
  private halfOpenInFlight = 0;
  private halfOpenSucceeded = 0;
  private listeners: ((from: CircuitState, to: CircuitState) => void)[] = [];

  constructor(opts: CircuitBreakerOptions = {}) {
    this.name = opts.name ?? "default";
    this.failureRateThreshold = opts.failureRateThreshold ?? 0.5;
    this.slowCallRateThreshold = opts.slowCallRateThreshold ?? 1;
    this.slowCallDurationThresholdMs = opts.slowCallDurationThresholdMs ?? 2000;
    this.windowSize = Math.max(1, opts.slidingWindowSize ?? 20);
    this.minimumNumberOfCallsGuard(opts.minimumNumberOfCalls);
    this.minimumCalls = Math.min(opts.minimumNumberOfCalls ?? this.windowSize, this.windowSize);
    this.openDurationMs = opts.waitDurationInOpenStateMs ?? 10_000;
    this.halfOpenPermitted = Math.max(1, opts.permittedNumberOfCallsInHalfOpenState ?? 3);
    this.recordFailure = opts.recordFailurePredicate ?? (() => true);
    this.clock = opts.clock ?? systemClock;
  }

  private minimumNumberOfCallsGuard(v: number | undefined) {
    if (v !== undefined && v < 1) throw new RangeError("minimumNumberOfCalls must be >= 1");
  }

  getState(): CircuitState {
    // OPEN expiring is a passive transition — there is no timer, so it is
    // evaluated on read. A breaker nobody calls should not hold a timer open.
    if (this.state === "OPEN" && this.clock.now() - this.openedAt >= this.openDurationMs) {
      this.transition("HALF_OPEN");
    }
    return this.state;
  }

  getMetrics(): CircuitMetrics {
    const n = this.ring.length;
    const failures = this.ring.filter((o) => o === "failure").length;
    const slow = this.ring.filter((o) => o === "slow").length;
    return {
      state: this.state,
      failureRate: n === 0 ? 0 : failures / n,
      slowCallRate: n === 0 ? 0 : slow / n,
      bufferedCalls: n,
    };
  }

  onStateChange(fn: (from: CircuitState, to: CircuitState) => void): () => void {
    this.listeners.push(fn);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== fn);
    };
  }

  /** Run `fn` under the breaker. Rejects with CircuitOpenError without calling
   *  `fn` when the circuit is open. */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    const state = this.getState();

    if (state === "OPEN") {
      throw new CircuitOpenError(this.name, this.openedAt + this.openDurationMs, this.getMetrics());
    }

    if (state === "HALF_OPEN") {
      // Admit only the permitted number of probes. Excess callers are rejected
      // rather than queued — queueing them would recreate the pile-up the
      // breaker exists to prevent.
      if (this.halfOpenInFlight >= this.halfOpenPermitted) {
        throw new CircuitOpenError(this.name, this.openedAt + this.openDurationMs, this.getMetrics());
      }
      this.halfOpenInFlight++;
    }

    const started = this.clock.now();
    try {
      const result = await fn();
      this.record(this.clock.now() - started >= this.slowCallDurationThresholdMs ? "slow" : "success", state);
      return result;
    } catch (e) {
      this.record(this.recordFailure(e) ? "failure" : "success", state);
      throw e;
    }
  }

  private record(outcome: Outcome, stateAtCall: CircuitState): void {
    if (stateAtCall === "HALF_OPEN") {
      // A slow probe is not a healthy provider. Treat it as a failed probe.
      if (outcome === "success") {
        this.halfOpenSucceeded++;
        if (this.halfOpenSucceeded >= this.halfOpenPermitted) this.close();
      } else {
        this.open();
      }
      return;
    }

    // Fixed-size ring: index arithmetic, no array shifting.
    if (this.ring.length < this.windowSize) this.ring.push(outcome);
    else {
      this.ring[this.cursor] = outcome;
      this.cursor = (this.cursor + 1) % this.windowSize;
    }

    if (this.ring.length < this.minimumCalls) return;

    const m = this.getMetrics();
    if (m.failureRate >= this.failureRateThreshold || m.slowCallRate >= this.slowCallRateThreshold) {
      this.open();
    }
  }

  private open(): void {
    this.openedAt = this.clock.now();
    this.transition("OPEN");
  }

  private close(): void {
    this.ring = [];
    this.cursor = 0;
    this.transition("CLOSED");
  }

  private transition(to: CircuitState): void {
    const from = this.state;
    if (from === to) return;
    this.state = to;
    if (to === "HALF_OPEN") {
      this.halfOpenInFlight = 0;
      this.halfOpenSucceeded = 0;
    }
    // A throwing listener must not corrupt breaker state or bubble into the
    // caller's request — this is telemetry, not control flow.
    for (const l of this.listeners) {
      try {
        l(from, to);
      } catch {
        /* ignored deliberately */
      }
    }
  }

  /** Hold the circuit open — an operator kill switch for a provider known to be
   *  bad, independent of what its error rate currently looks like. */
  forceOpen(): void {
    this.openedAt = Number.MAX_SAFE_INTEGER;
    this.transition("OPEN");
  }

  /** Force closed. Does NOT clear history, so a genuinely broken provider trips
   *  again on the next window rather than appearing healthy. */
  forceClosed(): void {
    this.transition("CLOSED");
  }

  /** Back to a clean CLOSED with no recorded history. */
  reset(): void {
    this.ring = [];
    this.cursor = 0;
    this.openedAt = 0;
    this.transition("CLOSED");
  }
}
