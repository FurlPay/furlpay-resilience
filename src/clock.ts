// ---------------------------------------------------------------------------
// Injectable clock.
//
// Every time-dependent module here takes a Clock rather than calling Date.now()
// and setTimeout directly. That is what makes the tests deterministic without a
// fake-timer library: a test supplies a clock it controls and advances it by
// hand, so "wait 10 seconds for the breaker to half-open" is an assignment, not
// a sleep.
//
// This is stronger than patching global timers. Global fake timers are process-
// wide, leak between test files when a restore is missed, and cannot represent
// two components on different clocks. An injected clock is scoped to the object
// that holds it.
//
// Production uses `systemClock`, which is a thin pass-through with no overhead.
// ---------------------------------------------------------------------------

export interface Clock {
  /** Epoch milliseconds. */
  now(): number;
  /** Resolves after `ms`. Must reject if `signal` aborts, so a pending backoff
   *  does not outlive the request that scheduled it. */
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  sleep: (ms, signal) =>
    new Promise<void>((resolve, reject) => {
      if (signal?.aborted) return reject(abortError(signal));
      const id = setTimeout(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }, ms);

      // DELIBERATELY NOT unref'd. An earlier version called `id.unref()` to
      // avoid holding a Node process open, and it was wrong: an unref'd timer
      // does not keep the event loop alive, so a pending retry backoff was
      // silently dropped whenever nothing else was scheduled. The awaiting
      // promise then never settled and the process exited mid-retry — a
      // payment retry that vanishes without an error is the worst possible
      // failure here. A caller that needs to abandon a backoff passes a signal.

      function onAbort() {
        clearTimeout(id);
        reject(abortError(signal!));
      }
      signal?.addEventListener("abort", onAbort, { once: true });
    }),
};

function abortError(signal: AbortSignal): Error {
  const reason = (signal as { reason?: unknown }).reason;
  if (reason instanceof Error) return reason;
  const e = new Error("Aborted");
  e.name = "AbortError";
  return e;
}

/**
 * A clock the caller drives by hand. Test-only, but exported because consumers
 * writing their own resilience tests need the same tool.
 *
 * `advance` resolves every sleep whose deadline has passed, in deadline order,
 * and yields to the microtask queue between each so awaiting code actually runs
 * before the next timer fires. Resolving them all in one synchronous burst is
 * the classic fake-timer bug: continuations pile up behind a single tick and
 * observe a time that never existed.
 */
export class ManualClock implements Clock {
  private t: number;
  private pending: { at: number; resolve: () => void; reject: (e: Error) => void; signal?: AbortSignal }[] = [];

  constructor(startMs = 0) {
    this.t = startMs;
  }

  now(): number {
    return this.t;
  }

  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(abortError(signal));
    return new Promise<void>((resolve, reject) => {
      const entry = { at: this.t + ms, resolve, reject, signal };
      this.pending.push(entry);
      signal?.addEventListener(
        "abort",
        () => {
          this.pending = this.pending.filter((p) => p !== entry);
          reject(abortError(signal));
        },
        { once: true }
      );
    });
  }

  /**
   * Move time forward, firing due sleeps in order and letting their
   * continuations run.
   *
   * The subtlety that makes this work with real async code: a sleep is often
   * scheduled by a continuation that has not run yet when advance() is called.
   * A test does `const p = client.post(...)` and immediately `advance(1000)` —
   * at that moment the first fetch has not resolved, so no backoff exists to
   * fire, and a naive implementation would set the time and return, leaving the
   * sleep scheduled in the past and never resolved.
   *
   * So after draining the queue we yield and LOOK AGAIN, repeatedly, until a
   * full drain produces no new work. `idleRounds` bounds that so a pathological
   * sleep-in-a-loop cannot hang the test runner.
   */
  async advance(ms: number): Promise<void> {
    const target = this.t + ms;
    let idleRounds = 0;

    while (idleRounds < 8) {
      const due = this.pending.filter((p) => p.at <= target).sort((a, b) => a.at - b.at);

      if (due.length === 0) {
        // Nothing due right now — let pending continuations run and re-check,
        // since one of them may schedule a sleep inside this window.
        idleRounds++;
        await Promise.resolve();
        await Promise.resolve();
        continue;
      }

      idleRounds = 0;
      const next = due[0];
      this.pending = this.pending.filter((p) => p !== next);
      this.t = Math.max(this.t, next.at);
      next.resolve();
      // Let whatever was awaiting that sleep proceed before the next one fires.
      await Promise.resolve();
      await Promise.resolve();
    }

    this.t = target;
    await Promise.resolve();
  }

  /** Sleeps still outstanding. A test asserting "nothing is scheduled" uses this. */
  get pendingCount(): number {
    return this.pending.length;
  }
}
