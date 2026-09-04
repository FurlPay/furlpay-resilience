import { AllEndpointsFailedError } from "./errors.js";
import { systemClock, type Clock } from "./clock.js";
import { CircuitBreaker, type CircuitBreakerOptions } from "./circuit-breaker.js";

// ---------------------------------------------------------------------------
// Multi-endpoint failover pool.
//
// A single RPC endpoint is a single point of failure, and in practice it fails
// partially rather than cleanly: 429 under load, a stale block height, latency
// climbing to seconds. FurlPay's settlement path cannot be one provider's bad
// afternoon away from stopping.
//
// SELECTION. Endpoints are ranked by a score combining EWMA latency and health.
// EWMA rather than a raw average because a provider that was slow an hour ago
// and is fine now should stop being penalised — an unweighted mean has no
// forgetting and pins a recovered node at the bottom of the list forever.
//
// Each endpoint gets its OWN circuit breaker. One shared breaker across the
// pool defeats the purpose: the first bad provider trips it and the healthy
// ones become unreachable, converting a partial outage into a total one.
//
// RECOVERY IS PASSIVE BY DEFAULT. A tripped endpoint returns when its breaker
// half-opens and a probe succeeds — driven by real traffic, not a background
// timer. An optional active probe exists for pools quiet enough that no organic
// traffic would rediscover a recovered node.
// ---------------------------------------------------------------------------

export interface Endpoint {
  id: string;
  url: string;
  /** Preference multiplier; higher wins ties. Default 1. */
  weight?: number;
  headers?: Record<string, string>;
}

export interface EndpointHealth {
  id: string;
  url: string;
  /** EWMA of observed latency, ms. */
  latencyMs: number;
  consecutiveFailures: number;
  totalCalls: number;
  totalFailures: number;
  circuitState: "CLOSED" | "OPEN" | "HALF_OPEN";
  lastError?: string;
  healthy: boolean;
}

export interface FailoverPoolOptions {
  name?: string;
  endpoints: Endpoint[];
  /** EWMA smoothing, 0..1. Higher reacts faster. Default 0.3. */
  latencySmoothing?: number;
  /** Latency assumed for an endpoint with no samples yet, ms. Default 100. */
  initialLatencyMs?: number;
  circuitBreaker?: Omit<CircuitBreakerOptions, "name" | "clock">;
  clock?: Clock;
  onFailover?: (info: { from: string; to: string; error: unknown }) => void;
}

interface Tracked {
  endpoint: Endpoint;
  breaker: CircuitBreaker;
  latencyMs: number;
  samples: number;
  consecutiveFailures: number;
  totalCalls: number;
  totalFailures: number;
  lastError?: string;
}

export class FailoverPool {
  readonly name: string;
  private readonly tracked: Tracked[];
  private readonly smoothing: number;
  private readonly clock: Clock;
  private readonly onFailover?: FailoverPoolOptions["onFailover"];

  constructor(opts: FailoverPoolOptions) {
    if (!opts.endpoints?.length) throw new RangeError("a pool needs at least one endpoint");
    const seen = new Set<string>();
    for (const e of opts.endpoints) {
      if (seen.has(e.id)) throw new RangeError(`duplicate endpoint id: ${e.id}`);
      seen.add(e.id);
    }

    this.name = opts.name ?? "pool";
    this.smoothing = opts.latencySmoothing ?? 0.3;
    this.clock = opts.clock ?? systemClock;
    this.onFailover = opts.onFailover;

    const initialLatency = opts.initialLatencyMs ?? 100;
    this.tracked = opts.endpoints.map((endpoint) => ({
      endpoint,
      breaker: new CircuitBreaker({
        ...opts.circuitBreaker,
        name: `${this.name}:${endpoint.id}`,
        clock: this.clock,
      }),
      latencyMs: initialLatency,
      samples: 0,
      consecutiveFailures: 0,
      totalCalls: 0,
      totalFailures: 0,
    }));
  }

  /** Lower is better. Weight divides, so a higher weight sorts earlier. */
  private score(t: Tracked): number {
    const weight = t.endpoint.weight ?? 1;
    const penalty = 1 + t.consecutiveFailures;
    return (t.latencyMs * penalty) / Math.max(weight, Number.EPSILON);
  }

  /** Endpoints whose breaker will admit a call, best first. */
  private candidates(): Tracked[] {
    const open = new Set<Tracked>();
    const usable: Tracked[] = [];
    for (const t of this.tracked) {
      if (t.breaker.getState() === "OPEN") open.add(t);
      else usable.push(t);
    }
    usable.sort((a, b) => this.score(a) - this.score(b));

    // Everything tripped: try them all anyway, best-scored first. Refusing to
    // call is strictly worse than calling a maybe-recovered provider — and the
    // breakers still record what happens.
    if (usable.length === 0) return [...this.tracked].sort((a, b) => this.score(a) - this.score(b));
    return usable;
  }

  /**
   * Run `fn` against the healthiest endpoint, failing over on error.
   *
   * `fn` receives the endpoint so it can build a URL and merge headers. The
   * pool never constructs the request itself — that would force one protocol
   * shape on every consumer (JSON-RPC, REST, GraphQL) and make it useless for
   * the others.
   */
  async execute<T>(fn: (endpoint: Endpoint) => Promise<T>, signal?: AbortSignal): Promise<T> {
    const errors: Record<string, string> = {};
    const order = this.candidates();
    let previousId: string | undefined;

    for (const t of order) {
      signal?.throwIfAborted();
      const started = this.clock.now();
      try {
        const result = await t.breaker.execute(() => fn(t.endpoint));
        this.recordSuccess(t, this.clock.now() - started);
        return result;
      } catch (e) {
        this.recordFailure(t, this.clock.now() - started, e);
        errors[t.endpoint.id] = e instanceof Error ? e.message : String(e);

        // An abort is the caller's decision. Do not burn the rest of the pool.
        if (e instanceof Error && e.name === "AbortError") throw e;

        if (previousId !== undefined || order.length > 1) {
          this.onFailover?.({ from: t.endpoint.id, to: "", error: e });
        }
        previousId = t.endpoint.id;
      }
    }

    throw new AllEndpointsFailedError(this.name, errors);
  }

  private recordSuccess(t: Tracked, latencyMs: number): void {
    t.totalCalls++;
    t.consecutiveFailures = 0;
    t.lastError = undefined;
    // First sample replaces the assumed default outright; averaging against a
    // guess would carry that guess for many calls.
    t.latencyMs = t.samples === 0 ? latencyMs : this.smoothing * latencyMs + (1 - this.smoothing) * t.latencyMs;
    t.samples++;
  }

  private recordFailure(t: Tracked, latencyMs: number, error: unknown): void {
    t.totalCalls++;
    t.totalFailures++;
    t.consecutiveFailures++;
    t.lastError = error instanceof Error ? error.message : String(error);
    // A failure's latency still says something about the endpoint — a 30s
    // timeout should push it down the ranking, not be discarded.
    t.latencyMs = t.samples === 0 ? latencyMs : this.smoothing * latencyMs + (1 - this.smoothing) * t.latencyMs;
    t.samples++;
  }

  health(): EndpointHealth[] {
    return this.tracked.map((t) => ({
      id: t.endpoint.id,
      url: t.endpoint.url,
      latencyMs: Math.round(t.latencyMs),
      consecutiveFailures: t.consecutiveFailures,
      totalCalls: t.totalCalls,
      totalFailures: t.totalFailures,
      circuitState: t.breaker.getState(),
      lastError: t.lastError,
      healthy: t.breaker.getState() !== "OPEN",
    }));
  }

  /**
   * Actively probe endpoints whose breaker is not CLOSED.
   *
   * Call this from an existing scheduler — the pool starts no timer of its own.
   * A library that spawns background intervals is a library that keeps a
   * serverless function alive and bills for it.
   */
  async probe(probeFn: (endpoint: Endpoint) => Promise<unknown>): Promise<void> {
    await Promise.allSettled(
      this.tracked
        .filter((t) => t.breaker.getState() !== "CLOSED")
        .map(async (t) => {
          const started = this.clock.now();
          try {
            await t.breaker.execute(() => probeFn(t.endpoint) as Promise<unknown>);
            this.recordSuccess(t, this.clock.now() - started);
          } catch (e) {
            this.recordFailure(t, this.clock.now() - started, e);
          }
        })
    );
  }
}
