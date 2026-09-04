import test from "node:test";
import assert from "node:assert/strict";
import { parseFrame, ResilientEventSource } from "../src/event-stream.js";

test("parses a complete SSE frame", () => {
  const e = parseFrame("id: 42\nevent: settled\ndata: {\"tx\":\"0xabc\"}");
  assert.deepEqual(e, { id: "42", event: "settled", data: '{"tx":"0xabc"}' });
});

test("defaults the event name and joins multi-line data", () => {
  const e = parseFrame("data: line1\ndata: line2");
  assert.equal(e!.event, "message");
  assert.equal(e!.data, "line1\nline2");
});

test("strips exactly one leading space, per the spec", () => {
  assert.equal(parseFrame("data:  two-spaces")!.data, " two-spaces");
  assert.equal(parseFrame("data:none")!.data, "none");
});

test("comment-only frames yield no event", () => {
  // `:` keep-alives carry no event but DO count as activity for the watchdog.
  assert.equal(parseFrame(": keep-alive"), null);
  assert.equal(parseFrame(""), null);
});

test("handles CRLF from a proxy that rewrote line endings", () => {
  const e = parseFrame("id: 7\r\nevent: ping\r\ndata: ok");
  assert.deepEqual(e, { id: "7", event: "ping", data: "ok" });
});

test("resumes with Last-Event-ID after a reconnect", async () => {
  const seenHeaders: (Record<string, string> | undefined)[] = [];
  let connection = 0;

  const fetchImpl = (async (_url: unknown, init?: RequestInit) => {
    seenHeaders.push(init?.headers as Record<string, string>);
    connection++;
    if (connection === 1) {
      // Deliver one event carrying an id, then end the stream.
      const body = new ReadableStream({
        start(c) {
          c.enqueue(new TextEncoder().encode("id: evt-9\ndata: hello\n\n"));
          c.close();
        },
      });
      return new Response(body, { status: 200 });
    }
    return new Response(new ReadableStream({ start: (c) => c.close() }), { status: 200 });
  }) as unknown as typeof fetch;

  const events: string[] = [];
  const es = new ResilientEventSource({
    url: "https://stream.example/events",
    fetchImpl,
    baseDelayMs: 1,
    maxDelayMs: 1,
    random: () => 0,
    onEvent: (e) => events.push(e.data),
  });

  es.start();
  // Let the first connection deliver and the reconnect begin.
  for (let i = 0; i < 60 && connection < 2; i++) await new Promise((r) => setTimeout(r, 5));
  await es.close();

  assert.deepEqual(events, ["hello"]);
  assert.equal(es.getLastEventId(), "evt-9");
  assert.ok(connection >= 2, "a closed stream must reconnect");
  // The whole point: without this header the gap is silently lost.
  assert.equal(seenHeaders[1]?.["Last-Event-ID"], "evt-9");
});

test("close() is idempotent and leaves no running loop", async () => {
  const fetchImpl = (async () =>
    new Response(new ReadableStream({ start: (c) => c.close() }), { status: 200 })) as unknown as typeof fetch;
  const es = new ResilientEventSource({ url: "https://x.example", fetchImpl, baseDelayMs: 1, random: () => 0 });
  es.start();
  await es.close();
  await es.close();
  assert.equal(es.getState(), "closed");
});
