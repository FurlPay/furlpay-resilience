import { systemClock, type Clock } from "./clock.js";
import { backoffDelay } from "./retry.js";

// ---------------------------------------------------------------------------
// Resilient event streams.
//
// A long-lived stream fails differently from a request. It does not return an
// error — it goes quiet. The socket stays open, the connection looks alive, and
// no events arrive. On a price feed that silence is indistinguishable from "the
// market did not move", which is why a heartbeat watchdog is not optional:
// without one, a dead feed is a feed showing stale prices as live.
//
// RESUMPTION. SSE defines `Last-Event-ID`: the client sends the last id it saw
// and a compliant server replays from there. Reconnecting without it silently
// drops everything that happened during the gap — on a settlement feed, that is
// a missed settlement, not a missed pixel.
//
// Reconnects use the same full-jitter backoff as retries. When a provider drops
// every connection at once, synchronised reconnects are a self-inflicted DDoS.
// ---------------------------------------------------------------------------

export interface StreamEvent {
  id?: string;
  event: string;
  data: string;
}

export interface ResilientEventSourceOptions {
  url: string;
  headers?: Record<string, string>;
  /** Reconnect if nothing arrives for this long. 0 disables. Default 45000ms. */
  heartbeatTimeoutMs?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  /** Give up after this many consecutive failures. Default Infinity. */
  maxReconnects?: number;
  fetchImpl?: typeof fetch;
  clock?: Clock;
  random?: () => number;
  onEvent?: (e: StreamEvent) => void;
  onError?: (e: unknown) => void;
  onStateChange?: (state: StreamState) => void;
}

export type StreamState = "connecting" | "open" | "reconnecting" | "closed";

export class ResilientEventSource {
  private readonly opts: ResilientEventSourceOptions;
  private readonly clock: Clock;
  private readonly doFetch: typeof fetch;
  private readonly random: () => number;

  private controller?: AbortController;
  private state: StreamState = "closed";
  private lastEventId?: string;
  private consecutiveFailures = 0;
  private stopped = false;
  private loop?: Promise<void>;

  constructor(opts: ResilientEventSourceOptions) {
    this.opts = opts;
    this.clock = opts.clock ?? systemClock;
    const f = opts.fetchImpl ?? globalThis.fetch;
    if (typeof f !== "function") throw new Error("No fetch implementation available — pass `fetchImpl`.");
    this.doFetch = f.bind(globalThis);
    this.random = opts.random ?? Math.random;
  }

  getState(): StreamState {
    return this.state;
  }
  /** Last id seen — survives reconnects, and is what resumption is built on. */
  getLastEventId(): string | undefined {
    return this.lastEventId;
  }

  /** Begin consuming. Returns immediately; the loop runs in the background and
   *  is awaited by close(), so it is never a floating promise. */
  start(): void {
    if (this.loop) return;
    this.stopped = false;
    this.loop = this.run().catch((e) => {
      this.opts.onError?.(e);
      this.setState("closed");
    });
  }

  async close(): Promise<void> {
    this.stopped = true;
    this.controller?.abort();
    await this.loop?.catch(() => {});
    this.loop = undefined;
    this.setState("closed");
  }

  private setState(s: StreamState): void {
    if (this.state === s) return;
    this.state = s;
    try {
      this.opts.onStateChange?.(s);
    } catch {
      /* telemetry must not break the stream */
    }
  }

  private async run(): Promise<void> {
    const maxReconnects = this.opts.maxReconnects ?? Number.POSITIVE_INFINITY;

    while (!this.stopped) {
      this.setState(this.consecutiveFailures === 0 ? "connecting" : "reconnecting");
      try {
        await this.connectOnce();
        // A clean end still means the stream is gone. Reconnect unless closed.
        this.consecutiveFailures = 0;
      } catch (e) {
        if (this.stopped) break;
        this.consecutiveFailures++;
        this.opts.onError?.(e);
        if (this.consecutiveFailures > maxReconnects) {
          this.setState("closed");
          return;
        }
      }

      if (this.stopped) break;
      const delay = backoffDelay(
        Math.min(this.consecutiveFailures, 10),
        this.opts.baseDelayMs ?? 500,
        this.opts.maxDelayMs ?? 30_000,
        this.random
      );
      try {
        await this.clock.sleep(delay);
      } catch {
        break; // aborted while waiting
      }
    }
    this.setState("closed");
  }

  private async connectOnce(): Promise<void> {
    this.controller = new AbortController();
    const headers: Record<string, string> = {
      Accept: "text/event-stream",
      "Cache-Control": "no-cache",
      ...this.opts.headers,
    };
    // Resume rather than restart. Without this, the gap is lost silently.
    if (this.lastEventId) headers["Last-Event-ID"] = this.lastEventId;

    const res = await this.doFetch(this.opts.url, { headers, signal: this.controller.signal });
    if (!res.ok || !res.body) throw new Error(`SSE connect failed: HTTP ${res.status}`);

    this.setState("open");
    this.consecutiveFailures = 0;

    const heartbeatMs = this.opts.heartbeatTimeoutMs ?? 45_000;
    let lastActivity = this.clock.now();

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    try {
      for (;;) {
        if (this.stopped) return;

        const { value, done } = await reader.read();
        if (done) return;

        lastActivity = this.clock.now();
        buffer += decoder.decode(value, { stream: true });

        // SSE frames are separated by a blank line. Split on both line endings
        // — a proxy that rewrites LF to CRLF must not break parsing.
        let sep: number;
        while ((sep = findFrameBoundary(buffer)) !== -1) {
          const frame = buffer.slice(0, sep);
          buffer = buffer.slice(sep).replace(/^(\r?\n){2}/, "");
          const parsed = parseFrame(frame);
          if (!parsed) continue;
          if (parsed.id) this.lastEventId = parsed.id;
          try {
            this.opts.onEvent?.(parsed);
          } catch {
            /* a consumer bug must not kill the stream */
          }
        }

        if (heartbeatMs > 0 && this.clock.now() - lastActivity > heartbeatMs) {
          throw new Error(`No data for ${heartbeatMs}ms — assuming the stream is dead`);
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
      this.controller?.abort();
    }
  }
}

function findFrameBoundary(buf: string): number {
  const lf = buf.indexOf("\n\n");
  const crlf = buf.indexOf("\r\n\r\n");
  if (lf === -1) return crlf;
  if (crlf === -1) return lf;
  return Math.min(lf, crlf);
}

/** Parse one SSE frame. Returns null for comment-only frames (`:` keep-alives),
 *  which carry no event but DO count as activity for the watchdog. */
export function parseFrame(frame: string): StreamEvent | null {
  let id: string | undefined;
  let event = "message";
  const dataLines: string[] = [];

  for (const rawLine of frame.split(/\r?\n/)) {
    if (rawLine === "" || rawLine.startsWith(":")) continue;
    const colon = rawLine.indexOf(":");
    const field = colon === -1 ? rawLine : rawLine.slice(0, colon);
    // Exactly one optional leading space is stripped, per the SSE spec.
    let value = colon === -1 ? "" : rawLine.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);

    if (field === "id") id = value;
    else if (field === "event") event = value;
    else if (field === "data") dataLines.push(value);
  }

  if (dataLines.length === 0) return null;
  return { id, event, data: dataLines.join("\n") };
}
