import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";
import {
  shouldPublish,
  shouldPublishUdt,
  parseStatePayload,
  recordPublish,
  rbeState,
} from "./mqtt.ts";

function cleanup() {
  rbeState.clear();
}

// =============================================================================
// parseStatePayload
// =============================================================================

Deno.test("parseStatePayload - legacy ONLINE", () => {
  assertEquals(parseStatePayload("ONLINE"), true);
});

Deno.test("parseStatePayload - legacy OFFLINE", () => {
  assertEquals(parseStatePayload("OFFLINE"), false);
});

Deno.test("parseStatePayload - legacy case insensitive", () => {
  assertEquals(parseStatePayload("online"), true);
  assertEquals(parseStatePayload("Online"), true);
  assertEquals(parseStatePayload("offline"), false);
});

Deno.test("parseStatePayload - Sparkplug B 3.0 JSON online", () => {
  assertEquals(parseStatePayload('{"online":true,"timestamp":1775147386540}'), true);
});

Deno.test("parseStatePayload - Sparkplug B 3.0 JSON offline", () => {
  assertEquals(parseStatePayload('{"online":false,"timestamp":1775147386540}'), false);
});

Deno.test("parseStatePayload - 3.0 JSON without timestamp", () => {
  assertEquals(parseStatePayload('{"online":true}'), true);
  assertEquals(parseStatePayload('{"online":false}'), false);
});

Deno.test("parseStatePayload - garbage string", () => {
  assertEquals(parseStatePayload("garbage"), false);
  assertEquals(parseStatePayload(""), false);
});

Deno.test("parseStatePayload - JSON without online field falls through to legacy", () => {
  assertEquals(parseStatePayload('{"status":"up"}'), false);
});

// =============================================================================
// shouldPublish — RBE logic
// =============================================================================

Deno.test("shouldPublish - first publish always true", () => {
  cleanup();
  assertEquals(shouldPublish("var1", 42), true);
});

Deno.test("shouldPublish - same value suppressed", () => {
  cleanup();
  recordPublish("var1", 42);
  assertEquals(shouldPublish("var1", 42), false);
});

Deno.test("shouldPublish - changed value publishes", () => {
  cleanup();
  recordPublish("var1", 42);
  assertEquals(shouldPublish("var1", 43), true);
});

Deno.test("shouldPublish - boolean RBE", () => {
  cleanup();
  recordPublish("b1", true);
  assertEquals(shouldPublish("b1", true), false);
  assertEquals(shouldPublish("b1", false), true);
});

Deno.test("shouldPublish - string RBE", () => {
  cleanup();
  recordPublish("s1", "hello");
  assertEquals(shouldPublish("s1", "hello"), false);
  assertEquals(shouldPublish("s1", "world"), true);
});

Deno.test("shouldPublish - disableRBE always publishes", () => {
  cleanup();
  recordPublish("var1", 42);
  assertEquals(shouldPublish("var1", 42, undefined, true), true);
});

Deno.test("shouldPublish - deadband suppresses small change", () => {
  cleanup();
  recordPublish("var1", 100);
  assertEquals(shouldPublish("var1", 100.5, { value: 1 }), false);
});

Deno.test("shouldPublish - deadband allows large change", () => {
  cleanup();
  recordPublish("var1", 100);
  assertEquals(shouldPublish("var1", 102, { value: 1 }), true);
});

Deno.test("shouldPublish - deadband exact threshold does not publish", () => {
  cleanup();
  recordPublish("var1", 100);
  // abs(101 - 100) = 1, not > 1
  assertEquals(shouldPublish("var1", 101, { value: 1 }), false);
});

Deno.test("shouldPublish - maxTime forces publish even with no change", () => {
  cleanup();
  // Simulate old publish time
  rbeState.set("var1", { lastPublishedValue: 42, lastPublishedTime: Date.now() - 10000 });
  assertEquals(shouldPublish("var1", 42, { value: 100, maxTime: 5000 }), true);
});

Deno.test("shouldPublish - minTime suppresses even with change", () => {
  cleanup();
  rbeState.set("var1", { lastPublishedValue: 100, lastPublishedTime: Date.now() });
  assertEquals(shouldPublish("var1", 200, { value: 1, minTime: 5000 }), false);
});

Deno.test("shouldPublish - object comparison via JSON", () => {
  cleanup();
  recordPublish("o1", JSON.stringify({ a: 1 }));
  assertEquals(shouldPublish("o1", JSON.stringify({ a: 1 })), false);
  assertEquals(shouldPublish("o1", JSON.stringify({ a: 2 })), true);
});

// =============================================================================
// shouldPublishUdt — Per-member deadband logic
// =============================================================================

Deno.test("shouldPublishUdt - first publish always true", () => {
  cleanup();
  assertEquals(
    shouldPublishUdt("udt1", { temp: 72, pressure: 14.7 }, { temp: { value: 1 } }),
    true,
  );
});

Deno.test("shouldPublishUdt - all members within deadband suppressed", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: JSON.stringify({ temp: 72, pressure: 14.7 }),
    lastPublishedTime: Date.now(),
  });
  assertEquals(
    shouldPublishUdt(
      "udt1",
      { temp: 72.5, pressure: 14.7 },
      { temp: { value: 1 }, pressure: { value: 0.5 } },
    ),
    false,
  );
});

Deno.test("shouldPublishUdt - one member exceeds deadband publishes", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: JSON.stringify({ temp: 72, pressure: 14.7 }),
    lastPublishedTime: Date.now(),
  });
  assertEquals(
    shouldPublishUdt(
      "udt1",
      { temp: 75, pressure: 14.7 },
      { temp: { value: 1 }, pressure: { value: 0.5 } },
    ),
    true,
  );
});

Deno.test("shouldPublishUdt - non-numeric member change publishes", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: JSON.stringify({ temp: 72, status: "running" }),
    lastPublishedTime: Date.now(),
  });
  assertEquals(
    shouldPublishUdt(
      "udt1",
      { temp: 72, status: "stopped" },
      { temp: { value: 1 } },
    ),
    true,
  );
});

Deno.test("shouldPublishUdt - removed member publishes", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: JSON.stringify({ temp: 72, pressure: 14.7 }),
    lastPublishedTime: Date.now(),
  });
  assertEquals(
    shouldPublishUdt("udt1", { temp: 72 }, { temp: { value: 1 } }),
    true,
  );
});

Deno.test("shouldPublishUdt - UDT-level maxTime forces publish", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: JSON.stringify({ temp: 72 }),
    lastPublishedTime: Date.now() - 10000,
  });
  assertEquals(
    shouldPublishUdt(
      "udt1",
      { temp: 72 },
      { temp: { value: 100 } },
      { value: 100, maxTime: 5000 },
    ),
    true,
  );
});

Deno.test("shouldPublishUdt - UDT-level minTime suppresses", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: JSON.stringify({ temp: 72 }),
    lastPublishedTime: Date.now(),
  });
  assertEquals(
    shouldPublishUdt(
      "udt1",
      { temp: 999 },
      { temp: { value: 0.1 } },
      { value: 0.1, minTime: 60000 },
    ),
    false,
  );
});

Deno.test("shouldPublishUdt - unparseable previous value publishes", () => {
  cleanup();
  rbeState.set("udt1", {
    lastPublishedValue: "not valid json {{{",
    lastPublishedTime: Date.now(),
  });
  assertEquals(
    shouldPublishUdt("udt1", { temp: 72 }, { temp: { value: 1 } }),
    true,
  );
});

// =============================================================================
// recordPublish
// =============================================================================

Deno.test("recordPublish - stores state in rbeState", () => {
  cleanup();
  recordPublish("rec1", 42);
  const state = rbeState.get("rec1");
  assertEquals(state?.lastPublishedValue, 42);
  assertEquals(typeof state?.lastPublishedTime, "number");
});
