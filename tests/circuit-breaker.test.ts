import test from "node:test";
import assert from "node:assert/strict";
import { CircuitBreaker, type CircuitBreakerOptions } from "../src/circuit-breaker.js";
import { CircuitOpenError } from "../src/errors.js";
import { ManualClock } from "../src/clock.js";

const fail = () => Promise.reject(new Error("boom"));
const ok = () => Promise.resolve("ok");

function breaker(clock: ManualClock, over: Partial<CircuitBreakerOptions> = {}) {
  return new CircuitBreaker({
    name: "test",
    slidingWindowSize: 4,
    failureRateThreshold: 0.5,
    waitDurationInOpenStateMs: 10_000,
    permittedNumberOfCallsInHalfOpenState: 2,
    clock,
    ...over,
  });
}

test("stays CLOSED until the window is full", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  // 3 failures in a window of 4 is a 100% failure rate on an incomplete window.
  // Tripping here would open the circuit on the first blip.
  for (let i = 0; i < 3; i++) await cb.execute(fail).catch(() => {});
  assert.equal(cb.getState(), "CLOSED");
});

test("opens once the failure rate is met over a full window", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});
  assert.equal(cb.getState(), "OPEN");
  assert.equal(cb.getMetrics().failureRate, 1);
});

test("does NOT open below the threshold", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  await cb.execute(fail).catch(() => {});
  await cb.execute(ok);
  await cb.execute(ok);
  await cb.execute(ok);
  assert.equal(cb.getState(), "CLOSED", "25% failure is under a 50% threshold");
});

test("OPEN rejects without invoking the downstream call", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});

  let invoked = false;
  await assert.rejects(
    () => cb.execute(async () => { invoked = true; return "x"; }),
    (e: unknown) => e instanceof CircuitOpenError
  );
  // The whole point: the downstream service is not touched while OPEN.
  assert.equal(invoked, false, "downstream must not be called while the circuit is open");
});

test("CircuitOpenError carries resetAt and metrics", async () => {
  const clock = new ManualClock(1_000);
  const cb = breaker(clock);
  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});
  const err = await cb.execute(ok).catch((e) => e);
  assert.ok(err instanceof CircuitOpenError);
  assert.equal(err.resetAt, 1_000 + 10_000);
  assert.equal(err.metrics.state, "OPEN");
  assert.equal(err.circuitName, "test");
});

test("transitions to HALF_OPEN after the wait, then CLOSED on successful probes", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});
  assert.equal(cb.getState(), "OPEN");

  await clock.advance(9_999);
  assert.equal(cb.getState(), "OPEN", "must not half-open early");

  await clock.advance(1);
  assert.equal(cb.getState(), "HALF_OPEN");

  await cb.execute(ok);
  assert.equal(cb.getState(), "HALF_OPEN", "one probe is not enough with 2 permitted");
  await cb.execute(ok);
  assert.equal(cb.getState(), "CLOSED");
});

test("a failed probe sends it back to OPEN and restarts the timer", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});
  await clock.advance(10_000);
  assert.equal(cb.getState(), "HALF_OPEN");

  await cb.execute(fail).catch(() => {});
  assert.equal(cb.getState(), "OPEN");

  await clock.advance(9_999);
  assert.equal(cb.getState(), "OPEN", "the wait restarts from the failed probe");
});

test("HALF_OPEN admits only the permitted number of probes", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});
  await clock.advance(10_000);

  let started = 0;
  const slow = () => { started++; return new Promise<string>(() => {}); }; // never settles
  void cb.execute(slow).catch(() => {});
  void cb.execute(slow).catch(() => {});
  await assert.rejects(() => cb.execute(slow), (e: unknown) => e instanceof CircuitOpenError);
  assert.equal(started, 2, "excess probes are rejected, not queued");
});

test("slow calls count as failures", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock, { slowCallDurationThresholdMs: 100, slowCallRateThreshold: 0.5 });
  // Succeeds, but takes 500ms of clock time — the outage that actually hurts.
  const slowOk = async () => { await clock.advance(500); return "ok"; };
  for (let i = 0; i < 4; i++) await cb.execute(slowOk);
  assert.equal(cb.getState(), "OPEN", "a provider answering 200 slowly must still trip the breaker");
});

test("recordFailurePredicate can exclude application errors", async () => {
  const clock = new ManualClock();
  // A 404 proves the provider is healthy and the resource is absent.
  const cb = breaker(clock, {
    recordFailurePredicate: (e: unknown) => (e as { status?: number })?.status !== 404,
  });
  const notFound = () => Promise.reject(Object.assign(new Error("nf"), { status: 404 }));
  for (let i = 0; i < 4; i++) await cb.execute(notFound).catch(() => {});
  assert.equal(cb.getState(), "CLOSED", "404s must not trip a breaker");
});

test("onStateChange fires, and a throwing listener cannot corrupt state", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  const seen: string[] = [];
  cb.onStateChange(() => { throw new Error("bad listener"); });
  cb.onStateChange((from, to) => seen.push(`${from}->${to}`));

  for (let i = 0; i < 4; i++) await cb.execute(fail).catch(() => {});
  assert.deepEqual(seen, ["CLOSED->OPEN"]);
  assert.equal(cb.getState(), "OPEN");
});

test("forceOpen holds regardless of the timer; reset clears history", async () => {
  const clock = new ManualClock();
  const cb = breaker(clock);
  cb.forceOpen();
  await clock.advance(10_000_000);
  assert.equal(cb.getState(), "OPEN", "a kill switch must not expire on its own");

  cb.reset();
  assert.equal(cb.getState(), "CLOSED");
  assert.equal(cb.getMetrics().bufferedCalls, 0);
});
