import test from "node:test";
import assert from "node:assert/strict";
import { ResilientHttpClient, type ResilienceMetric } from "../src/client.js";
import { AuthRefreshFailedError, HttpError, TimeoutError } from "../src/errors.js";
import { ManualClock } from "../src/clock.js";

interface Call { url: string; init: RequestInit }

/** A fetch stub that records calls and replays scripted responses. */
function stubFetch(script: (call: Call, n: number) => Response | Promise<Response>) {
  const calls: Call[] = [];
  const impl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const call = { url: String(input), init: init ?? {} };
    calls.push(call);
    return script(call, calls.length);
  }) as unknown as typeof fetch;
  return { impl, calls };
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

test("injects a trace header on every request", async () => {
  const { impl, calls } = stubFetch(() => json({ ok: true }));
  const client = new ResilientHttpClient({ baseUrl: "https://api.example", fetchImpl: impl });

  await client.get("/health");
  const headers = calls[0].init.headers as Record<string, string>;
  const trace = headers["X-FurlPay-Trace-Id"];
  // A UUID, not Math.random — this ends up in logs an operator trusts.
  assert.match(trace, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
});

test("trace ids are unique per request", async () => {
  const { impl, calls } = stubFetch(() => json({}));
  const client = new ResilientHttpClient({ baseUrl: "https://api.example", fetchImpl: impl });
  await client.get("/a");
  await client.get("/b");
  const a = (calls[0].init.headers as Record<string, string>)["X-FurlPay-Trace-Id"];
  const b = (calls[1].init.headers as Record<string, string>)["X-FurlPay-Trace-Id"];
  assert.notEqual(a, b);
});

test("refreshes the token once on 401 and replays the request", async () => {
  let refreshes = 0;
  const { impl, calls } = stubFetch((call) => {
    const auth = (call.init.headers as Record<string, string>)["Authorization"];
    return auth === "Bearer fresh" ? json({ ok: true }) : json({ error: "unauthorized" }, 401);
  });

  const client = new ResilientHttpClient({
    baseUrl: "https://api.example",
    fetchImpl: impl,
    auth: {
      getAccessToken: async () => "stale",
      refreshToken: async () => { refreshes++; return "fresh"; },
    },
  });

  assert.deepEqual(await client.get("/me"), { ok: true });
  assert.equal(refreshes, 1, "exactly one refresh");
  assert.equal(calls.length, 2, "original attempt plus one replay");
});

test("a persistent 401 refreshes only once, never in a loop", async () => {
  let refreshes = 0;
  const { impl, calls } = stubFetch(() => json({ error: "nope" }, 401));
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example",
    fetchImpl: impl,
    auth: { getAccessToken: async () => "a", refreshToken: async () => { refreshes++; return "b"; } },
  });

  await assert.rejects(() => client.get("/me"), (e: unknown) => e instanceof HttpError && e.status === 401);
  assert.equal(refreshes, 1, "refreshing per attempt would rate-limit the token endpoint during an incident");
  assert.equal(calls.length, 2);
});

test("a failing refresh surfaces as AuthRefreshFailedError, not the 401", async () => {
  const { impl } = stubFetch(() => json({}, 401));
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example",
    fetchImpl: impl,
    auth: {
      getAccessToken: async () => "a",
      refreshToken: async () => { throw new Error("token endpoint down"); },
      provider: "furlpay",
    },
  });

  const err = await client.get("/me").catch((e) => e);
  assert.ok(err instanceof AuthRefreshFailedError, "an operator needs to see WHERE it broke");
  assert.equal((err as AuthRefreshFailedError).provider, "furlpay");
});

test("a hung request becomes a TimeoutError", async () => {
  // Real fetch REJECTS when its signal aborts. A stub that ignores the signal
  // hangs forever and tells you nothing about the timeout path.
  const impl = (async (_u: unknown, init?: RequestInit) =>
    new Promise<Response>((_res, rej) => {
      init?.signal?.addEventListener("abort", () => {
        const e = new Error("The operation was aborted due to timeout");
        e.name = "TimeoutError";
        rej(e);
      });
    })) as unknown as typeof fetch;
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example", fetchImpl: impl, timeoutMs: 20, retry: { maxRetries: 0 },
  });
  const err = await client.get("/slow").catch((e) => e);
  assert.ok(err instanceof TimeoutError, `expected TimeoutError, got ${(err as Error)?.constructor?.name}`);
  assert.equal((err as TimeoutError).timeoutMs, 20);
});

test("a caller abort stays an abort and is not mislabelled a timeout", async () => {
  const ac = new AbortController();
  const impl = (async (_u: unknown, init?: RequestInit) =>
    new Promise<Response>((_res, rej) => {
      init?.signal?.addEventListener("abort", () => {
        const e = new Error("Aborted"); e.name = "AbortError"; rej(e);
      });
    })) as unknown as typeof fetch;

  const client = new ResilientHttpClient({
    baseUrl: "https://api.example", fetchImpl: impl, timeoutMs: 60_000, retry: { maxRetries: 0 },
  });
  const p = client.get("/x", { signal: ac.signal }).catch((e) => e);
  ac.abort();
  const err = await p;
  assert.ok(!(err instanceof TimeoutError), "the provider was not slow — the caller cancelled");
  assert.equal((err as Error).name, "AbortError");
});

test("an Idempotency-Key is forwarded and makes a POST retry-safe", async () => {
  const clock = new ManualClock();
  let n = 0;
  const { impl, calls } = stubFetch(() => (++n < 2 ? json({}, 503) : json({ ok: true })));

  const client = new ResilientHttpClient({
    baseUrl: "https://api.example",
    fetchImpl: impl,
    clock,
    retry: { maxRetries: 3, baseDelayMs: 10, random: () => 0 },
  });

  const p = client.post("/payments", { amount: 1 }, { idempotencyKey: "11111111-2222-3333-4444-555555555555" });
  await clock.advance(1000);
  assert.deepEqual(await p, { ok: true });
  assert.equal(calls.length, 2, "retried because a key was supplied");
  assert.equal((calls[0].init.headers as Record<string, string>)["Idempotency-Key"],
    "11111111-2222-3333-4444-555555555555");
});

test("a POST WITHOUT a key is not retried", async () => {
  const clock = new ManualClock();
  const { impl, calls } = stubFetch(() => json({}, 503));
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example", fetchImpl: impl, clock,
    retry: { maxRetries: 3, baseDelayMs: 10, random: () => 0 },
  });

  const p = client.post("/payments", { amount: 1 }).catch((e) => e);
  await clock.advance(1000);
  await p;
  assert.equal(calls.length, 1, "a lost ACK is not proof the charge failed");
});

test("an open circuit rejects without touching the network", async () => {
  const clock = new ManualClock();
  const { impl, calls } = stubFetch(() => json({}, 500));
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example", fetchImpl: impl, clock,
    retry: { maxRetries: 0 },
    circuitBreaker: { slidingWindowSize: 2, failureRateThreshold: 0.5, waitDurationInOpenStateMs: 5_000 },
  });

  for (let i = 0; i < 2; i++) await client.get("/x").catch(() => {});
  const before = calls.length;
  await client.get("/x").catch(() => {});
  assert.equal(calls.length, before, "no fetch while the circuit is open");
});

test("telemetry reports outcome and attempts without throwing into the request", async () => {
  const metrics: ResilienceMetric[] = [];
  const { impl } = stubFetch(() => json({ ok: true }));
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example",
    fetchImpl: impl,
    telemetry: {
      onMetric: (m) => { metrics.push(m); throw new Error("bad telemetry sink"); },
    },
  });

  assert.deepEqual(await client.get("/ok"), { ok: true }, "a broken sink must not fail the request");
  assert.equal(metrics.length, 1);
  assert.equal(metrics[0].outcome, "success");
  assert.equal(metrics[0].attempts, 1);
  assert.equal(metrics[0].method, "GET");
});

test("query parameters are encoded, and undefined is omitted", async () => {
  const { impl, calls } = stubFetch(() => json({}));
  const client = new ResilientHttpClient({ baseUrl: "https://api.example", fetchImpl: impl });
  await client.get("/search", { query: { q: "a b&c", limit: 10, cursor: undefined } });
  const url = new URL(calls[0].url);
  assert.equal(url.searchParams.get("q"), "a b&c");
  assert.equal(url.searchParams.get("limit"), "10");
  assert.equal(url.searchParams.has("cursor"), false);
});

test("a non-2xx becomes HttpError with a truncated body", async () => {
  const { impl } = stubFetch(() => new Response("x".repeat(2000), { status: 500 }));
  const client = new ResilientHttpClient({
    baseUrl: "https://api.example", fetchImpl: impl, retry: { maxRetries: 0 },
  });
  const err = await client.get("/boom").catch((e) => e);
  assert.ok(err instanceof HttpError);
  assert.equal((err as HttpError).status, 500);
  assert.equal((err as HttpError).bodySnippet.length, 512, "an upstream body is diagnostics, not a payload to hold");
});

test("204 resolves to undefined rather than failing to parse", async () => {
  const { impl } = stubFetch(() => new Response(null, { status: 204 }));
  const client = new ResilientHttpClient({ baseUrl: "https://api.example", fetchImpl: impl });
  assert.equal(await client.delete("/thing"), undefined);
});

test("refuses to construct when no fetch exists anywhere", () => {
  // `fetchImpl: undefined` correctly falls back to globalThis.fetch, which
  // exists on Node 18+. The contract under test is the runtime that has
  // NEITHER — an old Node, or a stripped Edge sandbox.
  const original = globalThis.fetch;
  try {
    (globalThis as { fetch?: unknown }).fetch = undefined;
    assert.throws(() => new ResilientHttpClient({ baseUrl: "https://x.example" }), /No fetch implementation/);
  } finally {
    (globalThis as { fetch?: unknown }).fetch = original;
  }
});
