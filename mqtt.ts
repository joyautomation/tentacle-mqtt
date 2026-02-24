/**
 * MQTT Sparkplug B bridge for tentacle-mqtt
 * Pure forwarder: publishes all NATS variable data to MQTT Sparkplug B.
 * Deadband/RBE settings come from the NATS message metadata (set by tentacle-plc).
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { disconnectNode, setValue, addMetrics, publishDeviceBirth, publishDeviceData } from "@joyautomation/synapse";
import type {
  SparkplugCreateNodeInput,
  SparkplugNode,
} from "@joyautomation/synapse";
import type { BridgeConfig } from "./types/config.ts";
import {
  createMetric,
  createTemplateDefinitionMetric,
  createTemplateInstanceMetric,
  flattenUdtToMetrics,
  sparkplugToPLCValue,
} from "./types/mappings.ts";
import type { UdtTemplateDefinition } from "@joyautomation/nats-schema";
import { NATS_SUBSCRIPTIONS, substituteTopic, NATS_TOPICS } from "@joyautomation/nats-schema";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("mqtt-bridge", LogLevel.debug);

type PlcVariable = {
  id: string;
  description: string;
  datatype: "number" | "boolean" | "string" | "udt";
  value: number | boolean | string | Record<string, unknown>;
  deadband?: { value: number; maxTime?: number };
  disableRBE?: boolean;
  /** The moduleId of the source module that published this variable */
  moduleId: string;
  /** Sparkplug B UDT template definition (only for datatype "udt") */
  udtTemplate?: UdtTemplateDefinition;
};

// =============================================================================
// RBE (Report By Exception) — local deadband checking
// Synapse's metricNeedsToPublish uses seconds; our maxTime is in milliseconds.
// We track publish state ourselves so the units stay consistent.
// =============================================================================

type RBEState = {
  lastPublishedValue: number | boolean | string | Record<string, unknown>;
  lastPublishedTime: number; // Date.now() ms
};

const rbeState = new Map<string, RBEState>();

// =============================================================================
// Template Definition Registry
// Tracks which Sparkplug B template definitions have been added to the node
// so we can include them in NBIRTH and avoid duplicate registration.
// =============================================================================
const knownTemplates = new Map<string, UdtTemplateDefinition>();

function shouldPublish(
  variableId: string,
  value: unknown,
  deadband?: { value: number; maxTime?: number },
  disableRBE?: boolean,
): boolean {
  if (!deadband) return true;
  if (disableRBE) return true;

  const state = rbeState.get(variableId);
  if (!state) return true; // never published — always publish

  const now = Date.now();
  const elapsed = now - state.lastPublishedTime;

  // maxTime exceeded — force publish
  if (deadband.maxTime && elapsed >= deadband.maxTime) return true;

  // Numeric deadband threshold
  if (typeof value === "number" && typeof state.lastPublishedValue === "number") {
    return Math.abs(value - state.lastPublishedValue) > deadband.value;
  }

  // Non-numeric — publish on any change
  return value !== state.lastPublishedValue;
}

function recordPublish(variableId: string, value: unknown): void {
  rbeState.set(variableId, {
    lastPublishedValue: value as number | boolean | string | Record<string, unknown>,
    lastPublishedTime: Date.now(),
  });
}

// =============================================================================
// Rebirth batching — collect new metrics and send ONE DBIRTH after a quiet period
// =============================================================================

const REBIRTH_DEBOUNCE_MS = 500;
let rebirthTimer: ReturnType<typeof setTimeout> | null = null;
let rebirthPending = false;

function scheduleRebirth(
  node: SparkplugNode,
  deviceId: string,
  config: BridgeConfig,
): void {
  rebirthPending = true;
  if (rebirthTimer) clearTimeout(rebirthTimer);
  rebirthTimer = setTimeout(() => {
    rebirthTimer = null;
    rebirthPending = false;
    if (!node.mqtt) return;
    const device = node.devices[deviceId];
    if (!device) return;
    const mqttConfig = {
      version: node.version || "spBv1.0",
      groupId: node.groupId,
      edgeNode: node.id,
    } as any;
    const birthPayload = {
      timestamp: Date.now(),
      metrics: Object.values(device.metrics).map(m => ({
        ...m,
        value: typeof m.value === "function" ? m.value() : m.value,
        timestamp: Date.now(),
      })),
    } as any;
    publishDeviceBirth(node, birthPayload, mqttConfig, node.mqtt, deviceId);
    log.info(`Published batched DBIRTH for device ${deviceId} with ${Object.keys(device.metrics).length} metrics`);
  }, REBIRTH_DEBOUNCE_MS);
}

/** Batch message format from tentacle-ethernetip */
type BatchMessage = {
  moduleId: string;
  deviceId: string;
  timestamp: number;
  values: Array<{
    variableId: string;
    value: number | boolean | string;
    datatype: string;
    deadband?: { value: number; maxTime?: number };
  }>;
};

/**
 * Process a batch message and publish a single DDATA with multiple metrics
 * This is much more efficient than publishing individual DDATA per metric
 */
async function processBatchMessage(
  data: BatchMessage,
  node: SparkplugNode,
  variables: Map<string, PlcVariable>,
  config: BridgeConfig,
  sourceModuleId: string,
): Promise<void> {
  const deviceId = config.mqtt.edgeNode;
  const now = Date.now();

  // Track which metrics to include in DDATA (variableId -> value)
  const ddataValues: Map<string, number | boolean | string> = new Map();

  // Variables requiring rebirth (new or type-changed)
  let needsRebirth = false;

  for (const item of data.values) {
    const { variableId, value } = item;

    // Get deadband from the NATS message
    const deadband = item.deadband;

    // Infer correct datatype from actual value
    let datatype = item.datatype as "number" | "boolean" | "string" | "udt";
    if (typeof value === "number" && datatype !== "number") {
      datatype = "number";
    } else if (typeof value === "boolean" && datatype !== "boolean") {
      datatype = "boolean";
    }

    // Update or create variable in local tracking
    const existing = variables.get(variableId);
    if (existing) {
      existing.value = value;
      existing.deadband = deadband;
      if (existing.datatype !== datatype) {
        existing.datatype = datatype;
      }
    } else {
      variables.set(variableId, {
        id: variableId,
        description: `${sourceModuleId}/${variableId}`,
        datatype,
        value,
        deadband,
        moduleId: sourceModuleId,
      });
    }

    // Check if metric exists
    const existingMetric = node.devices[deviceId]?.metrics[variableId];
    const isNewMetric = !existingMetric;

    // Check if existing metric has wrong Sparkplug type
    let needsTypeCorrection = false;
    if (existingMetric) {
      const expectedType = datatype === "number" ? "double" : datatype === "boolean" ? "boolean" : "string";
      if ((existingMetric.type as string) !== expectedType) {
        needsTypeCorrection = true;
      }
    }

    if (isNewMetric || needsTypeCorrection) {
      // New metric or type correction - add to node (will trigger rebirth later)
      const newMetric = createMetric(
        variableId,
        value,
        datatype,
        now,
        undefined,
        deadband,
        undefined,
        "plc",
        "good",
        sourceModuleId,
      );

      if (needsTypeCorrection) {
        log.info(`Batch: correcting metric type for ${variableId}`);
        delete node.devices[deviceId].metrics[variableId];
      } else {
        log.info(`Batch: new metric ${variableId}`);
      }

      addMetrics(node, { [variableId]: newMetric }, deviceId);
      recordPublish(variableId, value);
      needsRebirth = true;
    } else {
      // Existing metric — only add to DDATA batch if RBE check passes
      const batchVar = variables.get(variableId);
      if (shouldPublish(variableId, value, batchVar?.deadband)) {
        ddataValues.set(variableId, value);
        recordPublish(variableId, value);
      }
    }
  }

  // If any new metrics were added, schedule batched rebirth
  if (needsRebirth) {
    scheduleRebirth(node, deviceId, config);
  }

  // Publish DDATA with all metrics that didn't need rebirth (skip if rebirth pending)
  if (ddataValues.size > 0 && node.mqtt && !rebirthPending) {
    const mqttConfig = {
      version: node.version || "spBv1.0",
      groupId: node.groupId,
      edgeNode: node.id,
    } as any;

    // Build metrics array from existing metrics with new values
    const ddataMetrics = [];
    for (const [variableId, value] of ddataValues) {
      const metric = node.devices[deviceId]?.metrics[variableId];
      if (metric) {
        ddataMetrics.push({
          ...metric,
          value,
          timestamp: now,
        });
      }
    }

    const ddataPayload = {
      timestamp: now,
      metrics: ddataMetrics,
    } as any;
    publishDeviceData(node, ddataPayload, mqttConfig, node.mqtt, deviceId);
    log.debug(`Published batch DDATA with ${ddataMetrics.length} metrics`);
  }
}

/**
 * Setup MQTT Sparkplug B bridge
 * Subscribes to all variable data from NATS and bridges to Sparkplug B.
 * Variables are added dynamically as they arrive (triggers rebirth).
 */
export async function setupSparkplugBridge(config: BridgeConfig) {
  log.info("Initializing Sparkplug B bridge...");

  // Connect to NATS
  const nc = await connect({
    servers: Array.isArray(config.nats.servers)
      ? config.nats.servers
      : [config.nats.servers],
    user: config.nats.user,
    pass: config.nats.pass,
    token: config.nats.token,
  });

  log.info("Connected to NATS");

  // Track discovered variables
  const variables = new Map<string, PlcVariable>();

  // Subscribe to all module data topics (*.data.>)
  const mqttDataTopic = NATS_SUBSCRIPTIONS.allData();

  log.info(`Subscribing to NATS topic: ${mqttDataTopic}`);
  const sub = nc.subscribe(mqttDataTopic);

  // Create Sparkplug B node (synapse) - starts with empty device
  const { createNode } = await import("@joyautomation/synapse");

  // Parse MQTT broker URL
  const brokerUrl = config.mqtt.brokerUrl.replace(/^mqtt:\/\//, "tcp://")
    .replace(/^mqtts:\/\//, "tls://");

  log.info(`Creating Sparkplug B node: ${config.mqtt.edgeNode}`);

  let node: SparkplugNode;
  try {
    const nodeConfig = {
      id: config.mqtt.edgeNode,
      brokerUrl,
      clientId: config.mqtt.clientId,
      groupId: config.mqtt.groupId,
      version: "spBv1.0",
      username: config.mqtt.username || "",
      password: config.mqtt.password || "",
      metrics: {
        platform: {
          name: "platform",
          value: "tentacle",
          type: "string" as never,
        },
      },
      devices: {
        [config.mqtt.edgeNode]: {
          id: config.mqtt.edgeNode,
          metrics: {}, // Start empty, variables added as they arrive
        },
      },
    } as SparkplugCreateNodeInput;

    node = createNode(nodeConfig);
  } catch (e) {
    log.error("Failed to create Sparkplug B node:", e);
    throw e;
  }

  // Metrics start empty — PLC data messages populate them as they arrive (with deadband).
  // Raw ethernetip data is skipped; the PLC processes it and republishes.
  const deviceId = config.mqtt.edgeNode;

  // Log MQTT connection events (useful for debugging)
  if (node.events) {
    node.events.on("error", (err: Error) => log.error("MQTT error:", err));
  }

  // Handle device commands (DCMD) from MQTT
  // Synapse 0.0.87+ preserves external event listeners during reconnection cycles
  // Event signature: (topic: SparkplugTopic, payload: UPayload)
  node.events.on("dcmd", async (topic: any, payload: { metrics?: any[] }) => {
    try {
      log.info(`Received DCMD for device ${topic.deviceId}`);

      if (payload.metrics) {
        for (const metric of payload.metrics) {
          const metricName = metric.name as string;
          let variable = variables.get(metricName);

          // Handle Template Instance DCMD: metric.value is a UTemplate with .metrics members
          // Each changed member is routed as an individual command to the source module.
          if (variable?.datatype === "udt" && variable.udtTemplate &&
              metric.value && typeof metric.value === "object" &&
              Array.isArray((metric.value as any).metrics)) {
            const templateMembers = (metric.value as any).metrics as Array<{ name: string; value: unknown }>;
            const sourceModuleId = variable.moduleId;
            for (const member of templateMembers) {
              const memberCommandSubject = `${sourceModuleId}.command.${metricName}/${member.name}`;
              log.info(`Template DCMD: ${memberCommandSubject} = ${member.value}`);
              nc.publish(memberCommandSubject, new TextEncoder().encode(String(member.value)));
            }
            continue;
          }

          // Scalar DCMD: try full metric name, fall back to last segment for backward compatibility
          let variableId = metricName;
          if (!variable && metricName.includes("/")) {
            variableId = metricName.split("/").pop()!;
            variable = variables.get(variableId);
          }

          // Route the command to the source module that owns this variable
          const sourceModuleId = variable?.moduleId ?? "ethernetip";
          const commandSubject = substituteTopic(NATS_TOPICS.module.command, {
            moduleId: sourceModuleId,
            variableId,
          });

          let convertedValue: number | boolean | string | Record<string, unknown>;
          if (variable) {
            convertedValue = sparkplugToPLCValue(metric.value, variable.datatype);
          } else {
            const rawValue = metric.value;
            if (typeof rawValue === "boolean") {
              convertedValue = rawValue;
            } else if (typeof rawValue === "number") {
              convertedValue = rawValue;
            } else if (typeof rawValue === "string") {
              convertedValue = rawValue;
            } else {
              convertedValue = String(rawValue);
            }
            log.info(`Variable ${variableId} not yet discovered, forwarding raw value to ${sourceModuleId}`);
          }

          log.info(`Publishing command to ${commandSubject}: ${convertedValue}`);
          nc.publish(commandSubject, new TextEncoder().encode(String(convertedValue)));

          if (variable) {
            variable.value = convertedValue;
            const deviceId = config.mqtt.edgeNode;
            if (node.devices[deviceId]?.metrics[metricName]) {
              await setValue(node, metricName, convertedValue, deviceId);
            } else if (node.devices[deviceId]?.metrics[variableId]) {
              await setValue(node, variableId, convertedValue, deviceId);
            }
          }
        }
      }
    } catch (err) {
      log.error("Error handling DCMD:", err);
    }
  });

  // Process NATS messages — publish all variables to MQTT
  // Supports both single-value and batch formats
  (async () => {
    for await (const msg of sub) {
      try {
        const rawData = JSON.parse(msg.string());

        // Extract moduleId from NATS subject: {moduleId}.data.{variableId}
        const subjectParts = msg.subject.split(".");
        const sourceModuleId = subjectParts[0];

        // Skip raw ethernetip data — the PLC processes it and republishes with deadband.
        // MQTT only needs the PLC-processed values, not the raw scanner output.
        if (sourceModuleId === "ethernetip") {
          continue;
        }

        // Detect batch vs single-value format
        if (rawData.values && Array.isArray(rawData.values)) {
          // Batch format: { moduleId, deviceId, timestamp, values: [...] }
          await processBatchMessage(rawData, node, variables, config, sourceModuleId);
          continue;
        }

        // Single-value format
        const data = rawData as {
          moduleId: string;
          variableId: string;
          value: unknown;
          timestamp: number;
          datatype: "number" | "boolean" | "string" | "udt";
          deadband?: { value: number; maxTime?: number };
          disableRBE?: boolean;
          description?: string;
          udtTemplate?: UdtTemplateDefinition;
        };

        const { variableId, value } = data;
        const deviceId = config.mqtt.edgeNode;

        // Get deadband from the NATS message
        const deadband = data.deadband;
        const disableRBE = data.disableRBE;

        // Infer correct datatype from actual value (workaround for UDT template parsing bug)
        let datatype = data.datatype;
        if (typeof value === "number" && datatype !== "number") {
          log.debug(`Correcting datatype for ${variableId}: ${datatype} -> number (value is ${value})`);
          datatype = "number";
        } else if (typeof value === "boolean" && datatype !== "boolean") {
          datatype = "boolean";
        }

        // Update or create variable in local tracking
        const existing = variables.get(variableId);
        if (existing) {
          existing.value = value as number | boolean | string | Record<string, unknown>;
          existing.deadband = deadband;
          if (disableRBE !== undefined) existing.disableRBE = disableRBE;
          if (data.udtTemplate) existing.udtTemplate = data.udtTemplate;
          // Update datatype if it was corrected (e.g., from template parsing fix)
          if (existing.datatype !== datatype) {
            log.debug(`Updating stored datatype for ${variableId}: ${existing.datatype} -> ${datatype}`);
            existing.datatype = datatype;
          }
        } else {
          variables.set(variableId, {
            id: variableId,
            description: `${sourceModuleId}/${variableId}`,
            datatype,
            value: value as number | boolean | string | Record<string, unknown>,
            deadband,
            disableRBE,
            moduleId: sourceModuleId,
            udtTemplate: data.udtTemplate,
          });
        }

        // ── Template path (UDT with template definition) ──────────────────────
        const udtTemplate = data.udtTemplate;
        if (datatype === "udt" && udtTemplate) {
          const udtValue = value as Record<string, unknown>;

          if (config.useTemplates) {
            // Register template definition on the device (for DBIRTH) if not known
            if (!knownTemplates.has(udtTemplate.name)) {
              knownTemplates.set(udtTemplate.name, udtTemplate);
              const defMetric = createTemplateDefinitionMetric(udtTemplate);
              addMetrics(node, { [udtTemplate.name]: defMetric }, deviceId);
              log.info(`Registered Sparkplug B template definition: ${udtTemplate.name}`);
            }

            // Check if this Template Instance metric already exists on the device
            const existingTpl = node.devices[deviceId]?.metrics[variableId];
            const isNewTpl = !existingTpl;

            const instanceMetric = createTemplateInstanceMetric(
              variableId,
              udtValue,
              udtTemplate,
              Date.now(),
              "plc",
              "good",
              sourceModuleId,
              data.description,
            );

            if (isNewTpl) {
              log.info(`New template instance: ${variableId} (${udtTemplate.name})`);
              addMetrics(node, { [variableId]: instanceMetric }, deviceId);
              recordPublish(variableId, JSON.stringify(udtValue));
              scheduleRebirth(node, deviceId, config);
            } else {
              // Publish only when content changes (compare JSON strings for equality)
              if (shouldPublish(variableId, JSON.stringify(udtValue), undefined)) {
                recordPublish(variableId, JSON.stringify(udtValue));
                // Update stored metric value so future DBIRTH (rebirth) uses current data
                if (node.devices[deviceId]?.metrics[variableId]) {
                  node.devices[deviceId].metrics[variableId].value = instanceMetric.value as never;
                }
                if (node.mqtt && !rebirthPending) {
                  const mqttConfig = { version: node.version || "spBv1.0", groupId: node.groupId, edgeNode: node.id } as any;
                  publishDeviceData(node, { timestamp: Date.now(), metrics: [{ ...instanceMetric, timestamp: Date.now() }] } as any, mqttConfig, node.mqtt, deviceId);
                  log.debug(`Published DDATA template instance: ${variableId}`);
                }
              }
            }
          } else {
            // Flat mode: expand UDT members into individual metrics
            const flatMetrics = flattenUdtToMetrics(variableId, udtValue, udtTemplate, Date.now(), "plc", "good", sourceModuleId);
            for (const [flatName, flatMetric] of flatMetrics) {
              const existingFlat = node.devices[deviceId]?.metrics[flatName];
              const isNewFlat = !existingFlat;
              if (isNewFlat) {
                log.info(`New flat metric (from UDT): ${flatName}`);
                addMetrics(node, { [flatName]: flatMetric }, deviceId);
                recordPublish(flatName, flatMetric.value);
              } else if (shouldPublish(flatName, flatMetric.value, undefined)) {
                recordPublish(flatName, flatMetric.value);
                if (node.mqtt && !rebirthPending) {
                  const mqttConfig = { version: node.version || "spBv1.0", groupId: node.groupId, edgeNode: node.id } as any;
                  publishDeviceData(node, { timestamp: Date.now(), metrics: [{ ...flatMetric, timestamp: Date.now() }] } as any, mqttConfig, node.mqtt, deviceId);
                }
              }
              if (isNewFlat) scheduleRebirth(node, deviceId, config);
            }
          }
          continue; // Skip the scalar metric path below
        }

        // ── Scalar path (number | boolean | string) ────────────────────────────
        // Check if metric exists and if its type needs to be updated
        const existingMetric = node.devices[deviceId].metrics[variableId];
        const isNewMetric = !existingMetric;

        // Check if existing metric has wrong Sparkplug type (requires rebirth to fix)
        let needsTypeCorrection = false;
        if (existingMetric) {
          const expectedType = datatype === "number" ? "double" : datatype === "boolean" ? "boolean" : "string";
          if ((existingMetric.type as string) !== expectedType) {
            log.info(`Metric ${variableId} type mismatch: current=${existingMetric.type}, expected=${expectedType}. Will recreate metric.`);
            needsTypeCorrection = true;
          }
        }

        if (isNewMetric || needsTypeCorrection) {
          const newMetric = createMetric(
            variableId,
            value,
            datatype,
            Date.now(),
            undefined,
            deadband,
            disableRBE,
            "plc",
            "good",
            sourceModuleId,
            data.description,
          );

          if (needsTypeCorrection) {
            log.info(`Correcting metric type for ${variableId}: recreating with type=${newMetric.type}`);
            // Delete old metric with wrong type
            delete node.devices[deviceId].metrics[variableId];
          } else {
            log.info(`New metric: ${variableId}, adding to device (deadband: ${JSON.stringify(deadband)})`);
          }

          addMetrics(node, { [variableId]: newMetric }, deviceId);
          recordPublish(variableId, value);

          // Schedule batched rebirth (collects all new metrics over 500ms window)
          scheduleRebirth(node, deviceId, config);
        }

        // Update value via setValue (in-memory only — does not publish)
        await setValue(node, variableId, value as any, deviceId);

        // Publish DDATA only if RBE check passes
        // Skip if we just added a new metric or have a pending rebirth
        const trackedVar = variables.get(variableId);
        if (node.mqtt && !isNewMetric && !needsTypeCorrection && !rebirthPending &&
            shouldPublish(variableId, value, trackedVar?.deadband, trackedVar?.disableRBE)) {
          const metric = node.devices[deviceId]?.metrics[variableId];
          if (metric) {
            recordPublish(variableId, value);
            const mqttConfig = {
              version: node.version || "spBv1.0",
              groupId: node.groupId,
              edgeNode: node.id,
            } as any;
            const ddataPayload = {
              timestamp: Date.now(),
              metrics: [{
                ...metric,
                value,
                timestamp: Date.now(),
              }],
            } as any;
            publishDeviceData(node, ddataPayload, mqttConfig, node.mqtt, deviceId);
            log.debug(`Published DDATA for ${variableId} = ${value}`);
          }
        }
      } catch (error) {
        log.error("Error processing NATS message:", error);
      }
    }
  })();

  log.info("Bridge ready — all variables from NATS will be published to MQTT");

  return {
    variables,
    sparkplugNode: node,
    natsConnection: nc,
    disconnect: async () => {
      log.info("Disconnecting bridge...");
      sub.unsubscribe();
      disconnectNode(node);
      await nc.close();
      log.info("Bridge disconnected");
    },
  };
}
