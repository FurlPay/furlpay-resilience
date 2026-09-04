import test from "node:test";
import assert from "node:assert/strict";
import { FailoverPool } from "../src/failover-pool.js";
import { AllEndpointsFailedError } from "../src/errors.js";
import { ManualClock } from "../src/clock.js";

const endpoints = [
  { id: "alchemy", url: "https://a.example" },
  { id: "quicknode", url: "https://q.example" },
  { id: "helius", url: "https://h.example" },
];

test("a primary failure fails over to the next endpoint", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({ name: "rpc", endpoints, clock });
  const tried: string[] = [];

  const result = await pool.execute(async (e) => {
    tried.push(e.id);
    if (e.id === "alchemy") throw new Error("429 Too Many Requests");
    return `slot from ${e.id}`;
  });

  assert.equal(result, "slot from quicknode");
  assert.deepEqual(tried, ["alchemy", "quicknode"], "stops at the first success");
});

test("all endpoints failing raises AllEndpointsFailedError with every reason", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({ name: "rpc", endpoints, clock });

  const err = await pool.execute(async (e) => { throw new Error(`down: ${e.id}`); }).catch((x) => x);
  assert.ok(err instanceof AllEndpointsFailedError);
  assert.equal(err.poolName, "rpc");
  assert.deepEqual(Object.keys(err.errorsMap).sort(), ["alchemy", "helius", "quicknode"]);
  assert.match(err.errorsMap.alchemy, /down: alchemy/);
  assert.equal(err.retryable, true, "a whole-pool outage may be transient");
});

test("each endpoint gets its own breaker — one bad node cannot disable the pool", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({
    name: "rpc",
    endpoints,
    clock,
    circuitBreaker: { slidingWindowSize: 2, failureRateThreshold: 0.5, waitDurationInOpenStateMs: 10_000 },
  });

  // Trip alchemy specifically.
  for (let i = 0; i < 2; i++) {
    await pool.execute(async (e) => {
      if (e.id === "alchemy") throw new Error("bad");
      return "ok";
    }).catch(() => {});
  }

  const health = pool.health();
  const alchemy = health.find((h) => h.id === "alchemy")!;
  assert.equal(alchemy.circuitState, "OPEN");
  assert.ok(health.filter((h) => h.healthy).length >= 2, "the healthy nodes stay usable");

  // A tripped endpoint is skipped entirely, not merely deprioritised.
  const tried: string[] = [];
  await pool.execute(async (e) => { tried.push(e.id); return "ok"; });
  assert.ok(!tried.includes("alchemy"), "an OPEN endpoint must not be called");
});

test("routing prefers the lower-latency endpoint", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({ name: "rpc", endpoints: endpoints.slice(0, 2), clock, initialLatencyMs: 100 });

  // Give quicknode a fast history and alchemy a slow one.
  for (let i = 0; i < 5; i++) {
    await pool.execute(async (e) => {
      await clock.advance(e.id === "quicknode" ? 5 : 400);
      return "ok";
    }).catch(() => {});
  }

  const tried: string[] = [];
  await pool.execute(async (e) => { tried.push(e.id); return "ok"; });
  const health = pool.health();
  const q = health.find((h) => h.id === "quicknode")!;
  const a = health.find((h) => h.id === "alchemy")!;
  assert.ok(q.latencyMs < a.latencyMs, `quicknode ${q.latencyMs}ms should beat alchemy ${a.latencyMs}ms`);
  assert.equal(tried[0], "quicknode", "the fastest healthy endpoint is tried first");
});

test("weight breaks ties in favour of a preferred provider", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({
    name: "rpc",
    endpoints: [
      { id: "cheap", url: "https://c.example", weight: 1 },
      { id: "preferred", url: "https://p.example", weight: 10 },
    ],
    clock,
  });
  const tried: string[] = [];
  await pool.execute(async (e) => { tried.push(e.id); return "ok"; });
  assert.equal(tried[0], "preferred");
});

test("an abort does not burn through the rest of the pool", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({ name: "rpc", endpoints, clock });
  const tried: string[] = [];

  await pool.execute(async (e) => {
    tried.push(e.id);
    const err = new Error("Aborted");
    err.name = "AbortError";
    throw err;
  }).catch(() => {});

  assert.equal(tried.length, 1, "the caller cancelled; do not try every provider");
});

test("health reflects real observations", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({ name: "rpc", endpoints: endpoints.slice(0, 1), clock });
  await pool.execute(async () => { throw new Error("nope"); }).catch(() => {});

  const h = pool.health()[0];
  assert.equal(h.totalCalls, 1);
  assert.equal(h.totalFailures, 1);
  assert.equal(h.consecutiveFailures, 1);
  assert.match(h.lastError!, /nope/);

  await pool.execute(async () => "ok");
  assert.equal(pool.health()[0].consecutiveFailures, 0, "a success clears the streak");
  assert.equal(pool.health()[0].lastError, undefined);
});

test("probe only touches endpoints that are not CLOSED", async () => {
  const clock = new ManualClock();
  const pool = new FailoverPool({
    name: "rpc",
    endpoints: endpoints.slice(0, 2),
    clock,
    circuitBreaker: { slidingWindowSize: 1, failureRateThreshold: 0.5, waitDurationInOpenStateMs: 1_000 },
  });

  await pool.execute(async (e) => {
    if (e.id === "alchemy") throw new Error("bad");
    return "ok";
  }).catch(() => {});
  assert.equal(pool.health().find((h) => h.id === "alchemy")!.circuitState, "OPEN");

  await clock.advance(1_000); // alchemy half-opens
  const probed: string[] = [];
  await pool.probe(async (e) => { probed.push(e.id); return "ok"; });
  assert.deepEqual(probed, ["alchemy"], "a healthy endpoint needs no probe");
});

test("rejects a malformed pool rather than failing later", () => {
  assert.throws(() => new FailoverPool({ endpoints: [] }), RangeError);
  assert.throws(
    () => new FailoverPool({ endpoints: [{ id: "a", url: "x" }, { id: "a", url: "y" }] }),
    RangeError,
    "duplicate ids would make health tracking meaningless"
  );
});
