// ---------------------------------------------------------------------------
// Typed error hierarchy.
//
// Every failure mode gets its own class carrying the context a caller needs to
// DECIDE, not just to log. "Request failed" tells an operator nothing;
// CircuitOpenError with a resetAt tells them when it will be tried again, and
// tells the calling code whether waiting is even worth it.
//
// The discriminator is `code`, not `instanceof`. A library loaded twice (two
// copies in a dependency tree, an ESM/CJS split) produces two distinct classes
// and `instanceof` silently returns false across the boundary — which on a
// payment path means a retry decision quietly inverts. `code` survives that.
// ---------------------------------------------------------------------------

export type ResilienceErrorCode =
  | "circuit_open"
  | "rate_limit_exceeded"
  | "timeout"
  | "max_retries_exceeded"
  | "all_endpoints_failed"
  | "auth_refresh_failed"
  | "http_error";

export class FurlPayResilienceError extends Error {
  readonly code: ResilienceErrorCode;
  /** Whether trying the SAME call again could plausibly succeed. Callers use
   *  this instead of re-deriving retry policy from a status code. */
  readonly retryable: boolean;

  constructor(code: ResilienceErrorCode, message: string, retryable: boolean) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    this.retryable = retryable;
    // Restores the prototype chain when compiled to ES5 targets.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** True for any error produced by this package, across duplicate copies. */
export function isResilienceError(e: unknown): e is FurlPayResilienceError {
  return (
    e instanceof Error &&
    typeof (e as FurlPayResilienceError).code === "string" &&
    typeof (e as FurlPayResilienceError).retryable === "boolean"
  );
}

export interface CircuitMetrics {
  state: "CLOSED" | "OPEN" | "HALF_OPEN";
  failureRate: number;
  slowCallRate: number;
  bufferedCalls: number;
}

export class CircuitOpenError extends FurlPayResilienceError {
  readonly circuitName: string;
  /** Epoch ms at which the breaker will admit a probe. */
  readonly resetAt: number;
  readonly metrics: CircuitMetrics;

  constructor(circuitName: string, resetAt: number, metrics: CircuitMetrics) {
    super("circuit_open", `Circuit "${circuitName}" is open until ${new Date(resetAt).toISOString()}`, false);
    this.circuitName = circuitName;
    this.resetAt = resetAt;
    this.metrics = metrics;
  }
}

export class RateLimitExceededError extends FurlPayResilienceError {
  readonly retryAfterMs: number;
  readonly limit: number;
  readonly windowMs: number;

  constructor(retryAfterMs: number, limit: number, windowMs: number) {
    super("rate_limit_exceeded", `Rate limit of ${limit}/${windowMs}ms exceeded; retry in ${retryAfterMs}ms`, true);
    this.retryAfterMs = retryAfterMs;
    this.limit = limit;
    this.windowMs = windowMs;
  }
}

export class TimeoutError extends FurlPayResilienceError {
  readonly timeoutMs: number;
  readonly url: string;

  constructor(timeoutMs: number, url: string) {
    super("timeout", `Request to ${url} exceeded ${timeoutMs}ms`, true);
    this.timeoutMs = timeoutMs;
    this.url = url;
  }
}

export class MaxRetriesExceededError extends FurlPayResilienceError {
  readonly attempts: number;
  readonly lastError: unknown;
  readonly url?: string;

  constructor(attempts: number, lastError: unknown, url?: string) {
    super(
      "max_retries_exceeded",
      `Gave up after ${attempts} attempt(s)${url ? ` to ${url}` : ""}: ${describe(lastError)}`,
      false
    );
    this.attempts = attempts;
    this.lastError = lastError;
    this.url = url;
  }
}

export class AllEndpointsFailedError extends FurlPayResilienceError {
  readonly poolName: string;
  /** endpoint id -> the error it produced on this attempt. */
  readonly errorsMap: Record<string, string>;

  constructor(poolName: string, errorsMap: Record<string, string>) {
    const ids = Object.keys(errorsMap);
    super("all_endpoints_failed", `All ${ids.length} endpoint(s) in pool "${poolName}" failed`, true);
    this.poolName = poolName;
    this.errorsMap = errorsMap;
  }
}

export class AuthRefreshFailedError extends FurlPayResilienceError {
  readonly statusCode?: number;
  readonly provider?: string;

  constructor(statusCode?: number, provider?: string) {
    super(
      "auth_refresh_failed",
      `Token refresh failed${provider ? ` for ${provider}` : ""}${statusCode ? ` (${statusCode})` : ""}`,
      false
    );
    this.statusCode = statusCode;
    this.provider = provider;
  }
}

/** A non-2xx response that policy decided not to retry (or ran out of retries on). */
export class HttpError extends FurlPayResilienceError {
  readonly status: number;
  readonly url: string;
  /** Response body, truncated. NEVER assume this is safe to surface to an end
   *  user — an upstream error can contain anything, including its own secrets. */
  readonly bodySnippet: string;

  constructor(status: number, url: string, bodySnippet = "", retryable = false) {
    super("http_error", `HTTP ${status} from ${url}`, retryable);
    this.status = status;
    this.url = url;
    this.bodySnippet = bodySnippet.slice(0, 512);
  }
}

function describe(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  return String(e);
}
