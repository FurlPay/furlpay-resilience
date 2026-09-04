import { MaxRetriesExceededError, isResilienceError } from "./errors.js";
import { systemClock, type Clock } from "./clock.js";

// ---------------------------------------------------------------------------
// Exponential backoff with full jitter.
//
// FULL jitter, not "exponential plus a little noise":
//
//     delay = random(0, min(maxDelayMs, baseDelayMs * 2^attempt))
//
// The point is decorrelation. When a provider returns 503 to a thousand callers
// at once, fixed backoff has all thousand retry in the same millisecond — the
// thundering herd that keeps the provider down. Sampling uniformly from the
// whole interval spreads them out. Partial jitter still leaves a spike at the
// interval's end; full jitter does not.
//
// TWO THINGS THIS REFUSES TO RETRY, and both matter more than the backoff:
//
//   1. Client errors. 400/401/403/404/422 mean the request is wrong. Retrying
//      it is wrong the same number of times, and burns the caller's rate limit
//      finding that out.
//
//   2. Non-idempotent methods without an idempotency key. Retrying a POST whose
//      response you never saw can charge twice. The request may well have
//      succeeded — the ACK was lost, not the transaction. Silence is not
//      failure, and treating it as failure is how a retry becomes a double
//      spend.
// ---------------------------------------------------------------------------

export const DEFAULT_RETRYABLE_STATUS = [408, 429, 500, 502, 503, 504] as const;
export const DEFAULT_NON_RETRYABLE_STATUS = [400, 401, 403, 404, 422] as const;

/** Methods safe to retry without an idempotency key, per RFC 9110. */
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"]);

export interface RetryOptions {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  retryableStatus?: readonly number[];
  nonRetryableStatus?: readonly number[];
  /** HTTP method of the call being retried, when there is one. */
  method?: string;
  /** True when the caller sends an Idempotency-Key, making POST/PATCH safe. */
  hasIdempotencyKey?: boolean;
  /** Escape hatch for a genuinely safe unsafe-method retry. Name says what it is. */
  allowUnsafeMethodRetry?: boolean;
  /** Cap on honouring a server's Retry-After. An upstream asking for an hour
   *  should not hang the caller for an hour. Default 60000ms. */
  maxRetryAfterMs?: number;
  /** Injected for deterministic tests. Must return [0,1). */
  random?: () => number;
  clock?: Clock;
  signal?: AbortSignal;
  onRetry?: (info: { attempt: number; delayMs: number; error: unknown }) => void;
}

/** Anything carrying an HTTP status this module can reason about. */
export interface StatusCarrier {
  status?: number;
  statusCode?: number;
  headers?: { get?(name: string): string | null } | Record<string, string | string[] | undefined>;
}

function statusOf(e: unknown): number | undefined {
  if (!e || typeof e !== "object") return undefined;
  const c = e as StatusCarrier;
  return typeof c.status === "number" ? c.status : typeof c.statusCode === "number" ? c.statusCode : undefined;
}

function headerOf(e: unknown, name: string): string | undefined {
  if (!e || typeof e !== "object") return undefined;
  const h = (e as StatusCarrier).headers;
  if (!h) return undefined;
  if (typeof (h as { get?: unknown }).get === "function") {
    return (h as { get(n: string): string | null }).get(name) ?? undefined;
  }
  const rec = h as Record<string, string | string[] | undefined>;
  const hit = rec[name] ?? rec[name.toLowerCase()];
  return Array.isArray(hit) ? hit[0] : hit;
}

/**
 * Parse `Retry-After`. RFC 9110 permits either delay-seconds or an HTTP-date,
 * and real providers send both — an implementation handling only the integer
 * form silently ignores the date form and hammers the provider it was told to
 * back off from.
 *
 * Returns milliseconds, or undefined when absent/unparseable. Never negative:
 * a date already in the past means "retry now", not "retry in the past".
 */
export function parseRetryAfter(value: string | undefined, nowMs: number): number | undefined {
  if (value === undefined) return undefined;
  const trimmed = value.trim();
  if (trimmed === "") return undefined;

  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000;

  const at = Date.parse(trimmed);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, at - nowMs);
}

/** The delay for `attempt` (0-based), before any Retry-After override. */
export function backoffDelay(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  random: () => number
): number {
  const ceiling = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
  return Math.floor(random() * ceiling);
}

/** Whether a failure is worth another attempt. */
export function isRetryableError(error: unknown, opts: RetryOptions = {}): boolean {
  if (opts.signal?.aborted) return false;

  // An abort is a decision, not a fault. Retrying one ignores the caller.
  if (error instanceof Error && error.name === "AbortError") return false;

  const retryable = opts.retryableStatus ?? DEFAULT_RETRYABLE_STATUS;
  const nonRetryable = opts.nonRetryableStatus ?? DEFAULT_NON_RETRYABLE_STATUS;

  const status = statusOf(error);
  if (status !== undefined) {
    if (nonRetryable.includes(status)) return false;
    if (retryable.includes(status)) return true;
    // Unknown 4xx is a client problem; unknown 5xx is a server problem.
    return status >= 500;
  }

  // Errors this package raised already carry the verdict.
  if (isResilienceError(error)) return error.retryable;

  // No status at all: a transport failure (DNS, reset, TLS). Worth retrying.
  return true;
}

/** Whether the METHOD permits a retry, independent of the error. */
export function isMethodRetryable(opts: RetryOptions = {}): boolean {
  const method = (opts.method ?? "GET").toUpperCase();
  if (IDEMPOTENT_METHODS.has(method)) return true;
  return Boolean(opts.hasIdempotencyKey || opts.allowUnsafeMethodRetry);
}

/**
 * Run `fn`, retrying per policy. `fn` receives the 0-based attempt number.
 *
 * Throws MaxRetriesExceededError when attempts run out; rethrows the original
 * error when policy says not to retry — a caller inspecting a 404 should see
 * the 404, not a wrapper.
 */
export async function executeWithRetry<T>(
  fn: (attempt: number) => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const maxRetries = options.maxRetries ?? 3;
  const baseDelayMs = options.baseDelayMs ?? 200;
  const maxDelayMs = options.maxDelayMs ?? 20_000;
  const maxRetryAfterMs = options.maxRetryAfterMs ?? 60_000;
  const random = options.random ?? Math.random;
  const clock = options.clock ?? systemClock;

  const methodAllows = isMethodRetryable(options);
  let lastError: unknown;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    options.signal?.throwIfAborted();
    try {
      return await fn(attempt);
    } catch (e) {
      lastError = e;

      const isLast = attempt === maxRetries;
      if (isLast || !methodAllows || !isRetryableError(e, options)) {
        // Out of attempts on a retryable error is a distinct outcome from "this
        // was never retryable" — only the first gets the wrapper.
        //
        // `attempt > 0` matters: with maxRetries = 0 nothing was ever retried,
        // so wrapping would report a retry exhaustion that never happened and
        // hide the actual error class from the caller.
        if (isLast && attempt > 0 && methodAllows && isRetryableError(e, options)) {
          throw new MaxRetriesExceededError(attempt + 1, e);
        }
        throw e;
      }

      // A server telling us when to come back beats our own guess.
      const retryAfter = parseRetryAfter(headerOf(e, "retry-after"), clock.now());
      const delayMs =
        retryAfter !== undefined
          ? Math.min(retryAfter, maxRetryAfterMs)
          : backoffDelay(attempt, baseDelayMs, maxDelayMs, random);

      options.onRetry?.({ attempt, delayMs, error: e });
      await clock.sleep(delayMs, options.signal);
    }
  }

  // Unreachable: the loop either returns or throws.
  throw new MaxRetriesExceededError(maxRetries + 1, lastError);
}
