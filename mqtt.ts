/**
 * MQTT Sparkplug B bridge for tentacle-mqtt
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
 * Watches mqtt-config KV bucket for real-time configuration updates
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { disconnectNode, setValue, addMetrics, publishDeviceBirth, publishDeviceData } from "@joyautomation/synapse";
import type {
  SparkplugCreateNodeInput,
  SparkplugNode,
} from "@joyautomation/synapse";
import type { BridgeConfig } from "./types/config.ts";
import {
  createMetric,
  sparkplugToPLCValue,
} from "./types/mappings.ts";
import { NATS_TOPICS, substituteTopic } from "@joyautomation/nats-schema";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("mqtt-bridge", LogLevel.debug);

type PlcVariable = {
  id: string;
  description: string;
  datatype: "number" | "boolean" | "string" | "udt";
  value: number | boolean | string | Record<string, unknown>;
  deadband?: { value: number; maxTime?: number };
  disableRBE?: boolean;
};

/** MQTT config structure from KV bucket (matches tentacle-graphql format) */
type MqttVariableConfig = {
  enabled: boolean;
  deadband?: { value: number; maxTime?: number };
};

type MqttDefaults = {
  deadband: { value: number; maxTime?: number };
};

// Variables are stored as a Record<variableId, config>
type MqttVariablesRecord = Record<string, MqttVariableConfig>;

/** Runtime MQTT config with lookup map */
type MqttConfigState = {
  defaults: { value: number; maxTime?: number | null };
  enabledVariables: Map<string, { deadband?: { value: number; maxTime?: number | null } | null }>;
};

/**
 * Setup MQTT Sparkplug B bridge
 * Subscribes to MQTT-enabled variables from NATS and bridges to Sparkplug B.
 * Watches mqtt-config KV bucket for real-time configuration changes.
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

  // Setup JetStream and KV for mqtt-config bucket
  const js = jetstream(nc);
  const kvm = new Kvm(js);
  const configBucketName = `mqtt-config-${config.projectId}`;

  // MQTT config state - updated in real-time from KV bucket
  const mqttConfigState: MqttConfigState = {
    defaults: { value: 0, maxTime: null },
    enabledVariables: new Map(),
  };

  // Load and watch mqtt-config bucket
  let configBucket: Awaited<ReturnType<typeof kvm.open>> | null = null;
  try {
    configBucket = await kvm.open(configBucketName);
    log.info(`Opened mqtt-config bucket: ${configBucketName}`);

    // Load initial config
    await loadMqttConfig(configBucket, mqttConfigState);

    // Watch for config changes
    watchMqttConfig(configBucket, mqttConfigState);
  } catch (e) {
    log.warn(`Could not open mqtt-config bucket (${configBucketName}): ${e}. All variables will be published.`);
  }

  // Track discovered variables
  const variables = new Map<string, PlcVariable>();

  // Subscribe to all PLC data topics
  const mqttDataTopic = substituteTopic(NATS_TOPICS.plc.data, {
    projectId: config.projectId,
    variableId: "*",
  });

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
      metrics: {},
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

  // Request current variables from tentacle-ethernetip and populate initial metrics
  const deviceId = config.mqtt.edgeNode;
  try {
    const variablesSubject = `plc.variables.${config.projectId}`;
    log.info(`Requesting current variables from ${variablesSubject}...`);

    const response = await nc.request(variablesSubject, new TextEncoder().encode(""), { timeout: 5000 });
    const allVariables = JSON.parse(new TextDecoder().decode(response.data)) as Array<{
      deviceId: string;
      variableId: string;
      value: unknown;
      datatype: "number" | "boolean" | "string" | "udt";
      quality: string;
      source: string;
      lastUpdated: number;
    }>;

    log.info(`Received ${allVariables.length} variables from PLC scanner`);

    // Filter to only MQTT-enabled variables
    const enabledVariables = configBucket
      ? allVariables.filter(v => mqttConfigState.enabledVariables.has(v.variableId))
      : allVariables;

    log.info(`${enabledVariables.length} variables enabled for MQTT`);

    // Add each enabled variable to the device metrics
    for (const v of enabledVariables) {
      const variableConfig = mqttConfigState.enabledVariables.get(v.variableId);

      // Determine deadband config
      let deadband: { value: number; maxTime?: number } | undefined;
      if (variableConfig?.deadband) {
        deadband = {
          value: variableConfig.deadband.value,
          maxTime: variableConfig.deadband.maxTime ?? undefined,
        };
      } else if (configBucket) {
        deadband = {
          value: mqttConfigState.defaults.value,
          maxTime: mqttConfigState.defaults.maxTime ?? undefined,
        };
      }

      // Track the variable locally
      variables.set(v.variableId, {
        id: v.variableId,
        description: `${config.projectId}/${v.variableId}`,
        datatype: v.datatype,
        value: v.value as number | boolean | string | Record<string, unknown>,
        deadband,
      });

      // Add metric to device
      const metric = createMetric(
        v.variableId,
        v.value,
        v.datatype,
        Date.now(),
        undefined,
        deadband,
        undefined,
        "plc",
        "good",
      );
      addMetrics(node, { [v.variableId]: metric }, deviceId);
    }

    // Trigger initial DBIRTH with populated metrics
    if (node.mqtt && enabledVariables.length > 0) {
      const device = node.devices[deviceId];
      // Note: mqttConfig type cast needed due to synapse type mismatch (works at runtime)
      const mqttConfig = {
        version: node.version || "spBv1.0",
        groupId: node.groupId,
        edgeNode: node.id,
      } as any;
      const birthPayload = {
        timestamp: Date.now(),
        metrics: Object.values(device.metrics).map(m => ({
          ...m,
          timestamp: Date.now(),
        })),
      };
      publishDeviceBirth(node, birthPayload, mqttConfig, node.mqtt, deviceId);
      log.info(`Published initial DBIRTH with ${Object.keys(device.metrics).length} metrics`);
    }
  } catch (e) {
    log.warn(`Could not fetch initial variables: ${e}. Will populate as data arrives.`);
  }

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
          const variableId = metricName.includes("/")
            ? metricName.split("/").pop()!
            : metricName;
          const variable = variables.get(variableId);

          const commandSubject = `${config.projectId}/${variableId}`;

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
            log.info(`Variable ${variableId} not yet discovered, forwarding raw value`);
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

  // Process NATS messages - filter by MQTT config, add new variables dynamically
  (async () => {
    for await (const msg of sub) {
      try {
        const data = JSON.parse(msg.string()) as {
          projectId: string;
          variableId: string;
          value: unknown;
          timestamp: number;
          datatype: "number" | "boolean" | "string" | "udt";
          deadband?: { value: number; maxTime?: number };
          disableRBE?: boolean;
        };

        const { variableId, value } = data;
        const deviceId = config.mqtt.edgeNode;

        // Check if this variable is enabled for MQTT publishing
        // If no config bucket exists, publish all variables (backwards compatibility)
        const variableConfig = mqttConfigState.enabledVariables.get(variableId);
        if (configBucket && !variableConfig) {
          // Variable not enabled for MQTT - skip
          continue;
        }

        // Get deadband config from KV bucket (takes priority) or fall back to NATS message
        let deadband: { value: number; maxTime?: number } | undefined;
        if (variableConfig?.deadband) {
          deadband = {
            value: variableConfig.deadband.value,
            maxTime: variableConfig.deadband.maxTime ?? undefined,
          };
        } else if (configBucket) {
          // Use defaults from config
          deadband = {
            value: mqttConfigState.defaults.value,
            maxTime: mqttConfigState.defaults.maxTime ?? undefined,
          };
        } else if (data.deadband) {
          // Fall back to NATS message deadband (backwards compatibility)
          deadband = data.deadband;
        }

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
          // Update datatype if it was corrected (e.g., from template parsing fix)
          if (existing.datatype !== datatype) {
            log.debug(`Updating stored datatype for ${variableId}: ${existing.datatype} -> ${datatype}`);
            existing.datatype = datatype;
          }
        } else {
          variables.set(variableId, {
            id: variableId,
            description: `${config.projectId}/${variableId}`,
            datatype,
            value: value as number | boolean | string | Record<string, unknown>,
            deadband,
            disableRBE,
          });
        }

        // Check if metric exists and if its type needs to be updated
        const existingMetric = node.devices[deviceId].metrics[variableId];
        const isNewMetric = !existingMetric;

        // Check if existing metric has wrong Sparkplug type (requires rebirth to fix)
        // Sparkplug types: "double" for number, "boolean" for boolean
        let needsTypeCorrection = false;
        if (existingMetric) {
          const expectedType = datatype === "number" ? "double" : datatype === "boolean" ? "boolean" : "string";
          // Cast to string for comparison since TypeStr is a string literal union
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
          );

          if (needsTypeCorrection) {
            log.info(`Correcting metric type for ${variableId}: recreating with type=${newMetric.type}`);
            // Delete old metric with wrong type
            delete node.devices[deviceId].metrics[variableId];
          } else {
            log.info(`New metric: ${variableId}, adding to device (deadband: ${JSON.stringify(deadband)})`);
          }

          addMetrics(node, { [variableId]: newMetric }, deviceId);

          // Trigger device rebirth to update/add metric in DBIRTH
          if (node.mqtt) {
            const device = node.devices[deviceId];
            const mqttConfig = {
              version: node.version || "spBv1.0",
              groupId: node.groupId,
              edgeNode: node.id,
            } as any;
            const birthPayload = {
              timestamp: Date.now(),
              metrics: Object.values(device.metrics).map(m => ({
                ...m,
                timestamp: Date.now(),
              })),
            };
            publishDeviceBirth(node, birthPayload, mqttConfig, node.mqtt, deviceId);
            log.info(`Published DBIRTH for device ${deviceId} with ${Object.keys(device.metrics).length} metrics${needsTypeCorrection ? ' (type correction)' : ''}`);
          }
        }

        // Update value via setValue
        const result = await setValue(node, variableId, value, deviceId);

        // Manually publish DDATA since synapse's RBE may not trigger
        // Skip if we just published rebirth (new metric or type correction)
        if (node.mqtt && !isNewMetric && !needsTypeCorrection) {
          const metric = node.devices[deviceId]?.metrics[variableId];
          if (metric) {
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
            };
            publishDeviceData(node, ddataPayload, mqttConfig, node.mqtt, deviceId);
            log.debug(`Published DDATA for ${variableId} = ${value}`);
          }
        }
      } catch (error) {
        log.error("Error processing NATS message:", error);
      }
    }
  })();

  log.info("Bridge ready - variables will be added as they arrive from NATS (filtered by mqtt-config)");

  return {
    variables,
    sparkplugNode: node,
    natsConnection: nc,
    mqttConfigState,
    disconnect: async () => {
      log.info("Disconnecting bridge...");
      sub.unsubscribe();
      disconnectNode(node);
      await nc.close();
      log.info("Bridge disconnected");
    },
  };
}

/**
 * Load MQTT config from KV bucket into state
 * Keys used by tentacle-graphql:
 *   mqtt.defaults   - { deadband: { value, maxTime } }
 *   mqtt.variables  - { [variableId]: { enabled, deadband? } }
 */
async function loadMqttConfig(
  bucket: Awaited<ReturnType<Kvm["open"]>>,
  state: MqttConfigState,
): Promise<void> {
  // Load defaults
  try {
    const defaultsEntry = await bucket.get("mqtt.defaults");
    if (defaultsEntry?.value) {
      const defaults = defaultsEntry.json<MqttDefaults>();
      state.defaults = {
        value: defaults.deadband?.value ?? 0,
        maxTime: defaults.deadband?.maxTime ?? null,
      };
      log.info(`Loaded mqtt defaults: ${JSON.stringify(state.defaults)}`);
    }
  } catch (e) {
    log.warn(`Failed to load mqtt.defaults: ${e}`);
  }

  // Load variables
  try {
    const variablesEntry = await bucket.get("mqtt.variables");
    if (variablesEntry?.value) {
      const variables = variablesEntry.json<MqttVariablesRecord>();
      updateVariablesState(variables, state);
      log.info(`Loaded mqtt variables: ${state.enabledVariables.size} enabled`);
    }
  } catch (e) {
    log.warn(`Failed to load mqtt.variables: ${e}`);
  }
}

/**
 * Watch mqtt-config bucket for changes and update state in real-time
 */
async function watchMqttConfig(
  bucket: Awaited<ReturnType<Kvm["open"]>>,
  state: MqttConfigState,
): Promise<void> {
  try {
    // Watch all keys in the bucket
    const watch = await bucket.watch();

    (async () => {
      for await (const entry of watch) {
        const key = entry.key;

        if (key === "mqtt.defaults") {
          if (entry.value) {
            const defaults = entry.json<MqttDefaults>();
            state.defaults = {
              value: defaults.deadband?.value ?? 0,
              maxTime: defaults.deadband?.maxTime ?? null,
            };
            log.info(`mqtt.defaults updated: ${JSON.stringify(state.defaults)}`);
          } else {
            state.defaults = { value: 0, maxTime: null };
            log.info("mqtt.defaults deleted, using defaults");
          }
        } else if (key === "mqtt.variables") {
          if (entry.value) {
            const variables = entry.json<MqttVariablesRecord>();
            updateVariablesState(variables, state);
            log.info(`mqtt.variables updated: ${state.enabledVariables.size} enabled`);
          } else {
            state.enabledVariables.clear();
            log.info("mqtt.variables deleted, cleared enabled list");
          }
        }
      }
    })();

    log.info("Watching mqtt-config bucket for changes");
  } catch (e) {
    log.warn(`Failed to watch mqtt-config bucket: ${e}`);
  }
}

/**
 * Update enabled variables map from KV variables record
 */
function updateVariablesState(
  variables: MqttVariablesRecord,
  state: MqttConfigState,
): void {
  state.enabledVariables.clear();
  for (const [variableId, config] of Object.entries(variables)) {
    if (config.enabled) {
      state.enabledVariables.set(variableId, {
        deadband: config.deadband ? {
          value: config.deadband.value,
          maxTime: config.deadband.maxTime ?? null,
        } : null,
      });
    }
  }
}
