import { AuthRefreshFailedError, HttpError, TimeoutError } from "./errors.js";
import { systemClock, type Clock } from "./clock.js";
import { CircuitBreaker, type CircuitBreakerOptions } from "./circuit-breaker.js";
import { executeWithRetry, type RetryOptions } from "./retry.js";
import type { RateLimiter } from "./rate-limiter.js";
import type { FailoverPool } from "./failover-pool.js";

// ---------------------------------------------------------------------------
// ResilientHttpClient — the composition layer.
//
// Order matters, and this order is deliberate:
//
//   rate limit  →  circuit breaker  →  retry  →  timeout  →  fetch
//
//   Rate limit outermost, so a retry storm cannot exceed a provider's quota.
//   Breaker before retry, so an open circuit rejects instantly instead of
//     serving four retries of a call that cannot succeed.
//   Timeout innermost, so it bounds ONE attempt rather than the whole ladder —
//     otherwise the first slow attempt consumes the entire budget and the
//     retries never happen.
//
// A 401 gets ONE refresh-and-replay, outside the retry loop. Inside it, a
// provider rejecting a genuinely dead credential would trigger a refresh per
// attempt, which is how a token endpoint gets rate-limited during an incident.
// ---------------------------------------------------------------------------

export interface ResilienceMetric {
  traceId: string;
  method: string;
  url: string;
  status?: number;
  durationMs: number;
  attempts: number;
  outcome: "success" | "error";
  errorCode?: string;
  endpointId?: string;
}

export interface ResilientHttpClientConfig {
  baseUrl?: string;
  defaultHeaders?: Record<string, string>;
  /** Per-attempt timeout. Default 10000ms. */
  timeoutMs?: number;
  circuitBreaker?: CircuitBreakerOptions;
  retry?: Omit<RetryOptions, "method" | "hasIdempotencyKey" | "signal" | "clock">;
  rateLimiter?: RateLimiter;
  failoverPool?: FailoverPool;
  auth?: {
    getAccessToken: () => Promise<string | undefined>;
    refreshToken?: () => Promise<string | undefined>;
    /** Header name. Default "Authorization" with a Bearer prefix. */
    header?: string;
    scheme?: string;
    provider?: string;
  };
  telemetry?: {
    onMetric?: (metric: ResilienceMetric) => void;
    /** Default "X-FurlPay-Trace-Id". */
    traceHeader?: string;
  };
  /** Injected for tests. Defaults to globalThis.fetch. */
  fetchImpl?: typeof fetch;
  clock?: Clock;
}

export interface RequestOptions {
  method?: string;
  headers?: Record<string, string>;
  body?: unknown;
  query?: Record<string, string | number | boolean | undefined>;
  signal?: AbortSignal;
  timeoutMs?: number;
  /** Present ⇒ POST/PATCH become retry-safe. */
  idempotencyKey?: string;
  /** Return the Response instead of parsing. For streams and binaries. */
  raw?: boolean;
}

export class ResilientHttpClient {
  private readonly cfg: ResilientHttpClientConfig;
  private readonly breaker?: CircuitBreaker;
  private readonly clock: Clock;
  private readonly doFetch: typeof fetch;
  private readonly traceHeader: string;
  private cachedToken?: string;

  constructor(config: ResilientHttpClientConfig = {}) {
    this.cfg = config;
    this.clock = config.clock ?? systemClock;
    this.breaker = config.circuitBreaker
      ? new CircuitBreaker({ ...config.circuitBreaker, clock: this.clock })
      : undefined;
    const f = config.fetchImpl ?? globalThis.fetch;
    if (typeof f !== "function") {
      throw new Error("No fetch implementation available — pass `fetchImpl` (Node >= 18 has a global fetch).");
    }
    // Bind so an unbound global fetch does not throw on `this`.
    this.doFetch = f.bind(globalThis);
    this.traceHeader = config.telemetry?.traceHeader ?? "X-FurlPay-Trace-Id";
  }

  get<T = unknown>(path: string, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, { ...options, method: "GET" });
  }
  post<T = unknown>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, { ...options, method: "POST", body });
  }
  patch<T = unknown>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, { ...options, method: "PATCH", body });
  }
  put<T = unknown>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, { ...options, method: "PUT", body });
  }
  delete<T = unknown>(path: string, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, { ...options, method: "DELETE" });
  }

  async request<T = unknown>(path: string, options: RequestOptions = {}): Promise<T> {
    const method = (options.method ?? "GET").toUpperCase();
    // Trace ids are correlation identifiers that end up in logs an operator
    // trusts. crypto.randomUUID, never Math.random (AGENTS.md rule 6).
    const traceId = crypto.randomUUID();
    const started = this.clock.now();
    let attempts = 0;
    let endpointId: string | undefined;

    const run = async (): Promise<T> => {
      // 1. Rate limit outermost.
      if (this.cfg.rateLimiter) await this.cfg.rateLimiter.acquire(1, options.signal);

      const attemptOnce = async (): Promise<T> => {
        attempts++;
        if (this.cfg.failoverPool) {
          return this.cfg.failoverPool.execute(async (endpoint) => {
            endpointId = endpoint.id;
            return this.fetchOnce<T>(endpoint.url + path, method, options, traceId, endpoint.headers);
          }, options.signal);
        }
        return this.fetchOnce<T>(this.resolve(path), method, options, traceId);
      };

      // 2. Breaker, 3. retry, 4. timeout (inside fetchOnce).
      const withRetry = () =>
        executeWithRetry(attemptOnce, {
          ...this.cfg.retry,
          method,
          hasIdempotencyKey: Boolean(options.idempotencyKey),
          signal: options.signal,
          clock: this.clock,
        });

      return this.breaker ? this.breaker.execute(withRetry) : withRetry();
    };

    try {
      const result = await this.withAuthRetry(run);
      this.emit({
        traceId, method, url: this.resolve(path), durationMs: this.clock.now() - started,
        attempts, outcome: "success", endpointId,
      });
      return result;
    } catch (e) {
      this.emit({
        traceId, method, url: this.resolve(path), durationMs: this.clock.now() - started,
        attempts, outcome: "error",
        status: e instanceof HttpError ? e.status : undefined,
        errorCode: (e as { code?: string })?.code,
        endpointId,
      });
      throw e;
    }
  }

  /** One refresh-and-replay on 401, outside the retry loop. */
  private async withAuthRetry<T>(run: () => Promise<T>): Promise<T> {
    try {
      return await run();
    } catch (e) {
      const is401 = e instanceof HttpError && e.status === 401;
      if (!is401 || !this.cfg.auth?.refreshToken) throw e;

      let refreshed: string | undefined;
      try {
        refreshed = await this.cfg.auth.refreshToken();
      } catch (refreshErr) {
        // Fail with the refresh failure, not the 401 — an operator needs to see
        // that the token endpoint is the problem.
        throw new AuthRefreshFailedError(
          refreshErr instanceof HttpError ? refreshErr.status : undefined,
          this.cfg.auth.provider
        );
      }
      if (!refreshed) throw new AuthRefreshFailedError(undefined, this.cfg.auth.provider);

      this.cachedToken = refreshed;
      return run();
    }
  }

  private resolve(path: string): string {
    if (/^https?:\/\//i.test(path)) return path;
    const base = (this.cfg.baseUrl ?? "").replace(/\/+$/, "");
    return base + (path.startsWith("/") ? path : `/${path}`);
  }

  private async fetchOnce<T>(
    url: string,
    method: string,
    options: RequestOptions,
    traceId: string,
    endpointHeaders?: Record<string, string>
  ): Promise<T> {
    const timeoutMs = options.timeoutMs ?? this.cfg.timeoutMs ?? 10_000;

    const target = new URL(url);
    for (const [k, v] of Object.entries(options.query ?? {})) {
      if (v !== undefined) target.searchParams.set(k, String(v));
    }

    const headers: Record<string, string> = {
      ...this.cfg.defaultHeaders,
      ...endpointHeaders,
      ...options.headers,
      [this.traceHeader]: traceId,
    };
    if (options.idempotencyKey) headers["Idempotency-Key"] = options.idempotencyKey;
    if (options.body !== undefined && !headers["Content-Type"]) headers["Content-Type"] = "application/json";

    if (this.cfg.auth) {
      const token = this.cachedToken ?? (await this.cfg.auth.getAccessToken());
      if (token) {
        this.cachedToken = token;
        const name = this.cfg.auth.header ?? "Authorization";
        const scheme = this.cfg.auth.scheme ?? "Bearer";
        headers[name] = scheme ? `${scheme} ${token}` : token;
      }
    }

    // Per-attempt timeout, composed with any caller signal so either can abort.
    //
    // DELIBERATELY NOT AbortSignal.timeout(). Node unrefs that timer, so when a
    // request is the only pending work — the ordinary case in a serverless
    // handler — the timeout never fires, the hung fetch never rejects, and the
    // process exits with the promise still pending. A ref'd timer is the whole
    // point of a timeout: it must be able to wake the loop.
    const timeoutController = new AbortController();
    const timeoutId = setTimeout(() => {
      const e = new Error(`Timed out after ${timeoutMs}ms`);
      e.name = "TimeoutError";
      timeoutController.abort(e);
    }, timeoutMs);

    const signal = options.signal
      ? anySignal([options.signal, timeoutController.signal])
      : timeoutController.signal;

    let res: Response;
    try {
      res = await this.doFetch(target.toString(), {
        method,
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal,
      });
    } catch (e) {
      // A caller abort must surface as an abort; only OUR timeout becomes a
      // TimeoutError. Conflating them tells an operator the provider was slow
      // when the request was actually cancelled upstream.
      if (options.signal?.aborted) throw e;
      if (e instanceof Error && (e.name === "TimeoutError" || e.name === "AbortError")) {
        throw new TimeoutError(timeoutMs, target.toString());
      }
      throw e;
    } finally {
      // Always clear it. A completed request that leaves its timer pending
      // holds the event loop open for the rest of the timeout window.
      clearTimeout(timeoutId);
    }

    if (!res.ok) {
      // Body is read for diagnostics only, and truncated. It is never assumed
      // safe to surface: an upstream error can contain anything.
      const snippet = await res.text().catch(() => "");
      throw Object.assign(new HttpError(res.status, target.toString(), snippet), { headers: res.headers });
    }

    if (options.raw) return res as unknown as T;
    if (res.status === 204) return undefined as T;

    const contentType = res.headers.get("content-type") ?? "";
    if (!contentType.includes("json")) return (await res.text()) as unknown as T;
    return (await res.json()) as T;
  }

  private emit(metric: ResilienceMetric): void {
    try {
      this.cfg.telemetry?.onMetric?.(metric);
    } catch {
      // Telemetry must never break the request it is describing.
    }
  }

  /** Drop the cached token — call after an out-of-band credential rotation. */
  invalidateToken(): void {
    this.cachedToken = undefined;
  }
}

/** AbortSignal.any where available, with a listener-based fallback for older
 *  runtimes. Both paths clean up so a long-lived signal cannot leak listeners. */
function anySignal(signals: AbortSignal[]): AbortSignal {
  const native = (AbortSignal as unknown as { any?: (s: AbortSignal[]) => AbortSignal }).any;
  if (typeof native === "function") return native(signals);

  const controller = new AbortController();
  const onAbort = (s: AbortSignal) => () => {
    controller.abort((s as { reason?: unknown }).reason);
    for (const { sig, fn } of handlers) sig.removeEventListener("abort", fn);
  };
  const handlers = signals.map((sig) => ({ sig, fn: onAbort(sig) }));
  for (const { sig, fn } of handlers) {
    if (sig.aborted) {
      controller.abort((sig as { reason?: unknown }).reason);
      break;
    }
    sig.addEventListener("abort", fn, { once: true });
  }
  return controller.signal;
}
