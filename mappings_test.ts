import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";
import {
  plcToSparkplugType,
  createMetric,
  createTemplateDefinitionMetric,
  createTemplateInstanceMetric,
  flattenUdtToMetrics,
  sparkplugToPLCValue,
} from "./types/mappings.ts";
import type { UdtTemplateDefinition } from "@joyautomation/nats-schema";

// =============================================================================
// plcToSparkplugType
// =============================================================================

Deno.test("plcToSparkplugType - number → double", () => {
  assertEquals(plcToSparkplugType("number"), "double");
});

Deno.test("plcToSparkplugType - boolean → boolean", () => {
  assertEquals(plcToSparkplugType("boolean"), "boolean");
});

Deno.test("plcToSparkplugType - string → string", () => {
  assertEquals(plcToSparkplugType("string"), "string");
});

Deno.test("plcToSparkplugType - udt → template", () => {
  assertEquals(plcToSparkplugType("udt"), "template");
});

Deno.test("plcToSparkplugType - case insensitive", () => {
  assertEquals(plcToSparkplugType("Number"), "double");
  assertEquals(plcToSparkplugType("BOOLEAN"), "boolean");
});

Deno.test("plcToSparkplugType - unknown defaults to string", () => {
  assertEquals(plcToSparkplugType("unknown"), "string");
});

// =============================================================================
// createMetric
// =============================================================================

Deno.test("createMetric - number metric", () => {
  const m = createMetric("temp", 72.5, "number", 1000);
  assertEquals(m.name, "temp");
  assertEquals(m.value, 72.5);
  assertEquals(m.type, "double");
  assertEquals(m.timestamp, 1000);
});

Deno.test("createMetric - boolean metric", () => {
  const m = createMetric("running", true, "boolean");
  assertEquals(m.value, true);
  assertEquals(m.type, "boolean");
});

Deno.test("createMetric - string metric from object", () => {
  const m = createMetric("data", { a: 1 }, "string");
  assertEquals(m.value, '{"a":1}');
  assertEquals(m.type, "string");
});

Deno.test("createMetric - boolean from string", () => {
  const m = createMetric("b", "true", "boolean");
  assertEquals(m.value, true);
  const m2 = createMetric("b", "on", "boolean");
  assertEquals(m2.value, true);
  const m3 = createMetric("b", "no", "boolean");
  assertEquals(m3.value, false);
});

Deno.test("createMetric - includes metadata properties", () => {
  const m = createMetric("x", 1, "number", undefined, undefined, { value: 0.5, maxTime: 3000 }, false, "plc", "good", "mod1", "A description");
  assertEquals(m.properties?.datatype?.value, "number");
  assertEquals(m.properties?.source?.value, "plc");
  assertEquals(m.properties?.quality?.value, "good");
  assertEquals(m.properties?.deadbandValue?.value, 0.5);
  assertEquals(m.properties?.deadbandMaxTime?.value, 3000);
  assertEquals(m.properties?.moduleId?.value, "mod1");
  assertEquals(m.properties?.description?.value, "A description");
});

// =============================================================================
// sparkplugToPLCValue
// =============================================================================

Deno.test("sparkplugToPLCValue - number passthrough", () => {
  assertEquals(sparkplugToPLCValue(42, "number"), 42);
});

Deno.test("sparkplugToPLCValue - string to number", () => {
  assertEquals(sparkplugToPLCValue("3.14", "number"), 3.14);
});

Deno.test("sparkplugToPLCValue - boolean passthrough", () => {
  assertEquals(sparkplugToPLCValue(true, "boolean"), true);
});

Deno.test("sparkplugToPLCValue - string to boolean", () => {
  assertEquals(sparkplugToPLCValue("true", "boolean"), true);
  assertEquals(sparkplugToPLCValue("1", "boolean"), true);
  assertEquals(sparkplugToPLCValue("on", "boolean"), true);
  assertEquals(sparkplugToPLCValue("false", "boolean"), false);
});

Deno.test("sparkplugToPLCValue - string passthrough", () => {
  assertEquals(sparkplugToPLCValue("hello", "string"), "hello");
});

Deno.test("sparkplugToPLCValue - udt from JSON string", () => {
  const result = sparkplugToPLCValue('{"a":1}', "udt");
  assertEquals(result, { a: 1 });
});

Deno.test("sparkplugToPLCValue - udt from object", () => {
  const obj = { a: 1 };
  assertEquals(sparkplugToPLCValue(obj, "udt"), obj);
});

Deno.test("sparkplugToPLCValue - udt invalid JSON returns string", () => {
  assertEquals(sparkplugToPLCValue("not json", "udt"), "not json");
});

Deno.test("sparkplugToPLCValue - unknown type defaults to string", () => {
  assertEquals(sparkplugToPLCValue(42, "unknown"), "42");
});

// =============================================================================
// createTemplateDefinitionMetric
// =============================================================================

Deno.test("createTemplateDefinitionMetric - basic definition", () => {
  const template: UdtTemplateDefinition = {
    name: "MotorUDT",
    version: "2.0",
    members: [
      { name: "speed", datatype: "number" },
      { name: "running", datatype: "boolean" },
      { name: "status", datatype: "string" },
    ],
  };
  const m = createTemplateDefinitionMetric(template);
  assertEquals(m.name, "MotorUDT");
  assertEquals(m.type, "template");

  const val = m.value as any;
  assertEquals(val.isDefinition, true);
  assertEquals(val.version, "2.0");
  assertEquals(val.templateRef, undefined);
  assertEquals(val.metrics.length, 3);

  // Definition metrics have default values, not null
  assertEquals(val.metrics[0].name, "speed");
  assertEquals(val.metrics[0].value, 0);
  assertEquals(val.metrics[0].type, "double");
  assertEquals(val.metrics[0].timestamp, undefined); // no timestamp on definitions

  assertEquals(val.metrics[1].name, "running");
  assertEquals(val.metrics[1].value, false);

  assertEquals(val.metrics[2].name, "status");
  assertEquals(val.metrics[2].value, "");
});

Deno.test("createTemplateDefinitionMetric - default version", () => {
  const template: UdtTemplateDefinition = {
    name: "SimpleUDT",
    members: [{ name: "val", datatype: "number" }],
  };
  const val = createTemplateDefinitionMetric(template).value as any;
  assertEquals(val.version, "1.0");
});

Deno.test("createTemplateDefinitionMetric - nested template", () => {
  const inner: UdtTemplateDefinition = {
    name: "InnerUDT",
    version: "1.0",
    members: [{ name: "x", datatype: "number" }],
  };
  const outer: UdtTemplateDefinition = {
    name: "OuterUDT",
    members: [
      { name: "nested", datatype: "string", templateRef: "InnerUDT" },
      { name: "flag", datatype: "boolean" },
    ],
  };
  const allTemplates = new Map<string, UdtTemplateDefinition>([
    ["InnerUDT", inner],
    ["OuterUDT", outer],
  ]);

  const m = createTemplateDefinitionMetric(outer, allTemplates);
  const val = m.value as any;
  const nestedMetric = val.metrics[0];
  assertEquals(nestedMetric.name, "nested");
  assertEquals(nestedMetric.type, "template");
  assertEquals(nestedMetric.value.isDefinition, true);
  assertEquals(nestedMetric.value.version, "1.0");
  assertEquals(nestedMetric.value.templateRef, undefined); // omitted for definitions
  assertEquals(nestedMetric.value.metrics[0].name, "x");
  assertEquals(nestedMetric.value.metrics[0].value, 0);
});

// =============================================================================
// createTemplateInstanceMetric
// =============================================================================

Deno.test("createTemplateInstanceMetric - basic instance", () => {
  const template: UdtTemplateDefinition = {
    name: "MotorUDT",
    version: "2.0",
    members: [
      { name: "speed", datatype: "number" },
      { name: "running", datatype: "boolean" },
    ],
  };
  const ts = 1700000000000;
  const m = createTemplateInstanceMetric("Motor1", { speed: 1800, running: true }, template, ts);
  assertEquals(m.name, "Motor1");
  assertEquals(m.type, "template");
  assertEquals(m.timestamp, ts);

  const val = m.value as any;
  assertEquals(val.isDefinition, false);
  assertEquals(val.templateRef, "MotorUDT");
  assertEquals(val.version, undefined); // version omitted for instances

  assertEquals(val.metrics[0].value, 1800);
  assertEquals(val.metrics[0].timestamp, ts);
  assertEquals(val.metrics[1].value, true);
});

Deno.test("createTemplateInstanceMetric - null member value returns null (except boolean)", () => {
  const template: UdtTemplateDefinition = {
    name: "T",
    members: [
      { name: "num", datatype: "number" },
      { name: "flag", datatype: "boolean" },
    ],
  };
  const m = createTemplateInstanceMetric("i1", {}, template, 1000);
  const val = m.value as any;
  assertEquals(val.metrics[0].value, null); // number with missing value → null
  assertEquals(val.metrics[1].value, false); // boolean with missing value → false
});

Deno.test("createTemplateInstanceMetric - nested template instance", () => {
  const inner: UdtTemplateDefinition = {
    name: "InnerUDT",
    version: "1.0",
    members: [{ name: "x", datatype: "number" }],
  };
  const outer: UdtTemplateDefinition = {
    name: "OuterUDT",
    members: [
      { name: "nested", datatype: "string", templateRef: "InnerUDT" },
    ],
  };
  const allTemplates = new Map([["InnerUDT", inner], ["OuterUDT", outer]]);
  const ts = 1700000000000;

  const m = createTemplateInstanceMetric(
    "Inst1", { nested: { x: 42 } }, outer, ts, undefined, undefined, undefined, undefined, allTemplates,
  );
  const val = m.value as any;
  const nestedMetric = val.metrics[0];
  assertEquals(nestedMetric.value.isDefinition, false);
  assertEquals(nestedMetric.value.templateRef, "InnerUDT");
  assertEquals(nestedMetric.value.version, undefined); // no version on instances
  assertEquals(nestedMetric.value.metrics[0].value, 42);
});

// =============================================================================
// flattenUdtToMetrics
// =============================================================================

Deno.test("flattenUdtToMetrics - creates flat metrics", () => {
  const template: UdtTemplateDefinition = {
    name: "MotorUDT",
    members: [
      { name: "speed", datatype: "number" },
      { name: "running", datatype: "boolean" },
      { name: "status", datatype: "string" },
    ],
  };
  const result = flattenUdtToMetrics("Motor1", { speed: 1800, running: true, status: "ok" }, template, 1000);
  assertEquals(result.size, 3);

  const speed = result.get("Motor1/speed")!;
  assertEquals(speed.name, "Motor1/speed");
  assertEquals(speed.value, 1800);
  assertEquals(speed.type, "double");

  const running = result.get("Motor1/running")!;
  assertEquals(running.value, true);
  assertEquals(running.type, "boolean");

  const status = result.get("Motor1/status")!;
  assertEquals(status.value, "ok");
  assertEquals(status.type, "string");
});
