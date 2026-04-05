import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { StoreForwardBuffer } from "./store-forward.ts";

function createBuffer(overrides?: Partial<{ maxRecords: number; maxBytes: number; drainRate: number }>) {
  return new StoreForwardBuffer({
    maxRecords: overrides?.maxRecords ?? 100,
    maxBytes: overrides?.maxBytes ?? 1_000_000,
    drainRate: overrides?.drainRate ?? 1000,
  });
}

function makePayload(size: number): Uint8Array {
  return new Uint8Array(size);
}

// =============================================================================
// Buffering
// =============================================================================

Deno.test("add - returns false when host is online (publish directly)", () => {
  const buf = createBuffer();
  buf.destroy();
  const result = buf.add("topic", makePayload(10));
  assertEquals(result, false);
  assertEquals(buf.getState().bufferedRecords, 0);
});

Deno.test("add - buffers when host is offline", () => {
  const buf = createBuffer();
  buf.setPrimaryHostId("host1");
  // Force offline immediately (bypass debounce)
  (buf as any)._primaryHostOnline = false;

  const result = buf.add("topic", makePayload(10));
  assertEquals(result, true);
  assertEquals(buf.getState().bufferedRecords, 1);
  buf.destroy();
});

Deno.test("add - evicts oldest when maxRecords exceeded", () => {
  const buf = createBuffer({ maxRecords: 3 });
  buf.setPrimaryHostId("host1");
  (buf as any)._primaryHostOnline = false;

  buf.add("t1", makePayload(10));
  buf.add("t2", makePayload(10));
  buf.add("t3", makePayload(10));
  buf.add("t4", makePayload(10)); // should evict t1

  const state = buf.getState();
  assertEquals(state.bufferedRecords, 3);
  assertEquals(state.totalEvicted, 1);
  assertEquals(state.totalBuffered, 4);
  buf.destroy();
});

Deno.test("add - evicts when maxBytes exceeded", () => {
  const buf = createBuffer({ maxBytes: 25 });
  buf.setPrimaryHostId("host1");
  (buf as any)._primaryHostOnline = false;

  buf.add("t1", makePayload(10));
  buf.add("t2", makePayload(10));
  buf.add("t3", makePayload(10)); // total would be 30, evicts t1

  const state = buf.getState();
  assertEquals(state.bufferedRecords, 2);
  assertEquals(state.totalEvicted, 1);
  buf.destroy();
});

// =============================================================================
// State reporting
// =============================================================================

Deno.test("getState - reports correct percentages", () => {
  const buf = createBuffer({ maxRecords: 10, maxBytes: 100 });
  buf.setPrimaryHostId("host1");
  (buf as any)._primaryHostOnline = false;

  buf.add("t1", makePayload(25));
  buf.add("t2", makePayload(25));

  const state = buf.getState();
  assertEquals(state.bufferUsedPercentRecords, 20); // 2/10 * 100
  assertEquals(state.bufferUsedPercentBytes, 50);   // 50/100 * 100
  assertEquals(state.primaryHostId, "host1");
  assertEquals(state.primaryHostOnline, false);
  buf.destroy();
});

// =============================================================================
// Drain
// =============================================================================

Deno.test("startDrain - drains buffered records via callback", async () => {
  const buf = createBuffer({ drainRate: 1000 });
  buf.setPrimaryHostId("host1");
  (buf as any)._primaryHostOnline = false;

  buf.add("t1", makePayload(5));
  buf.add("t2", makePayload(5));

  const drained: { topic: string; isHistorical: boolean }[] = [];
  buf.setPublishCallback((topic, _payload, isHistorical) => {
    drained.push({ topic, isHistorical });
  });

  // Bring host online to trigger drain
  (buf as any)._primaryHostOnline = true;
  buf.startDrain();

  // Wait for drain interval to fire
  await new Promise((r) => setTimeout(r, 250));

  assertEquals(drained.length, 2);
  assertEquals(drained[0].topic, "t1");
  assertEquals(drained[0].isHistorical, true);
  assertEquals(drained[1].topic, "t2");
  assertEquals(drained[1].isHistorical, true);

  assertEquals(buf.getState().bufferedRecords, 0);
  assertEquals(buf.getState().totalDrained, 2);
  buf.destroy();
});

Deno.test("startDrain - no records is a no-op", () => {
  const buf = createBuffer();
  buf.startDrain();
  assertEquals(buf.getState().draining, false);
  buf.destroy();
});

// =============================================================================
// Publish rate tracking
// =============================================================================

Deno.test("getPublishRate - returns 0 with no publishes", () => {
  const buf = createBuffer();
  assertEquals(buf.getPublishRate(), 0);
  buf.destroy();
});

Deno.test("getPublishRate - tracks recent publishes", () => {
  const buf = createBuffer();
  buf.recordPublish(10);
  const rate = buf.getPublishRate();
  // 10 metrics in 10 second window = 1/s
  assertEquals(rate, 1);
  buf.destroy();
});

// =============================================================================
// handleStateChange
// =============================================================================

Deno.test("handleStateChange - ignores non-matching host", () => {
  const buf = createBuffer();
  buf.setPrimaryHostId("host1");
  buf.handleStateChange("other-host", false);
  assertEquals(buf.primaryHostOnline, true); // unchanged
  buf.destroy();
});

Deno.test("handleStateChange - online cancels pending offline", async () => {
  const buf = createBuffer();
  buf.setPrimaryHostId("host1");

  // Trigger offline (debounced)
  buf.handleStateChange("host1", false);
  // Immediately go back online — should cancel the offline debounce
  buf.handleStateChange("host1", true);

  // Wait past the debounce period
  await new Promise((r) => setTimeout(r, 2500));
  assertEquals(buf.primaryHostOnline, true);
  buf.destroy();
});
