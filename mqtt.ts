/**
 * MQTT Sparkplug B bridge for tentacle-mqtt
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { disconnectNode, setValue } from "@joyautomation/synapse";
import type {
  SparkplugCreateNodeInput,
  SparkplugNode,
} from "@joyautomation/synapse";
import type { BridgeConfig } from "./types/config.ts";
import {
  createMetric,
  plcToSparkplugType,
  sparkplugToPLCValue,
} from "./types/mappings.ts";
import { NATS_TOPICS, substituteTopic } from "@tentacle/nats-schema";
import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("mqtt-bridge", LogLevel.info);

type PlcVariable = {
  id: string;
  description: string;
  datatype: "number" | "boolean" | "string" | "udt";
  value: number | boolean | string | Record<string, unknown>;
  deadband?: { value: number; maxTime?: number };
  disableRBE?: boolean;
};

type KVEntry = {
  data: Uint8Array;
};

/**
 * Setup MQTT Sparkplug B bridge
 * Discovers PLC variables from NATS KV and creates a bidirectional bridge
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

  // Get JetStream for KV access
  const js = jetstream(nc);
  const jsm = await js.jetstreamManager();
  const kvBucketName = `plc-variables-${config.projectId}`;
  const streamName = `$KV_${kvBucketName}`;
  const subjectPrefix = `$KV.${kvBucketName}`;

  // Variables will be discovered from NATS messages
  log.info("Waiting for initial PLC variables from NATS...");
  const variables = new Map<string, PlcVariable>();

  // Subscribe to PLC data to collect initial variables
  const plcDataTopic = substituteTopic(NATS_TOPICS.plc.data, {
    projectId: config.projectId,
    variableId: "*",
  });

  log.info(`Subscribing to NATS topic: ${plcDataTopic}`);
  // Create a temporary subscription for initial variable collection
  const initSub = nc.subscribe(plcDataTopic);

  // Collect initial variables with timeout and early exit when stable
  const collectInitialVariables = new Promise<void>((resolve) => {
    let resolved = false;
    let stabilityCounter = 0;
    const stabilityThreshold = 10; // 10 iterations with no new variables = stable

    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        initSub.unsubscribe();
        resolve();
      }
    }, 5000); // Wait up to 5 seconds for initial variables

    (async () => {
      for await (const msg of initSub) {
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

          const { variableId, value, datatype, deadband, disableRBE } = data;
          if (!variables.has(variableId)) {
            variables.set(variableId, {
              id: variableId,
              description: `${config.projectId}/${variableId}`,
              datatype,
              value: value as
                | number
                | boolean
                | string
                | Record<string, unknown>,
              deadband,
              disableRBE,
            });
            log.debug(
              `Discovered variable during init: ${variableId} (${datatype})`,
            );
            stabilityCounter = 0; // Reset stability counter when we find a new variable
          } else {
            stabilityCounter++;
            // If we've seen the same variables repeated, they're stable
            if (stabilityCounter >= stabilityThreshold && variables.size > 0) {
              if (!resolved) {
                resolved = true;
                clearTimeout(timeout);
                initSub.unsubscribe();
                resolve();
              }
              break;
            }
          }
        } catch (e) {
          log.warn("Error parsing NATS message:", (e as Error).message);
        }
      }
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        resolve();
      }
    })();
  });

  // Wait for initial variables to arrive
  await collectInitialVariables;
  log.info(`Collected ${variables.size} variables for initialization`);

  // If we didn't discover any variables, request them from the PLC
  if (variables.size === 0) {
    const requestSubject = substituteTopic(NATS_TOPICS.plc.variablesRequest, {
      projectId: config.projectId,
    });
    log.info(`No variables discovered, requesting from PLC: ${requestSubject}`);
    nc.publish(requestSubject, "");
  }

  // Create a new subscription for ongoing updates
  const sub = nc.subscribe(plcDataTopic);

  // Create Sparkplug B node (synapse)
  const { createNode } = await import("@joyautomation/synapse");

  // Parse MQTT broker URL
  const brokerUrl = config.mqtt.brokerUrl.replace(/^mqtt:\/\//, "tcp://")
    .replace(
      /^mqtts:\/\//,
      "tls://",
    );

  log.info(`Creating Sparkplug B node: ${config.mqtt.edgeNode}`);

  // Pre-populate device metrics with discovered variables
  // createMetric returns Synapse SparkplugMetric objects compatible with node config
  const deviceMetrics: Record<string, unknown> = {};
  for (const [variableId, variable] of variables.entries()) {
    deviceMetrics[variableId] = createMetric(
      variable.id,
      variable.value,
      variable.datatype,
      Date.now(),
      undefined, // scanRate
      variable.deadband,
      variable.disableRBE,
      "plc", // source
      "good", // quality
    );
  }

  let node: SparkplugNode;
  try {
    // Build config object for synapse createNode
    // See: https://github.com/joyautomation/synapse
    const nodeConfig = {
      id: config.mqtt.edgeNode,
      brokerUrl,
      clientId: config.mqtt.clientId,
      groupId: config.mqtt.groupId,
      version: "spBv1.0",
      username: config.mqtt.username || "",
      password: config.mqtt.password || "",
      metrics: {}, // Root-level metrics (node birth)
      devices: {
        // Device data with pre-populated metrics
        [config.mqtt.edgeNode]: {
          id: config.mqtt.edgeNode,
          metrics: deviceMetrics,
        },
      },
    } as SparkplugCreateNodeInput;

    node = createNode(nodeConfig);
  } catch (e) {
    log.error("Failed to create Sparkplug B node:", e);
    throw e;
  }

  // Handle device commands (DCMD) from MQTT
  node.events?.on("dcmd", async (_topic: string, message: any) => {
    log.info("Received DCMD:", message);

    if (message.metrics) {
      for (const metric of message.metrics) {
        const metricName = metric.name as string;
        // Extract variable ID from metric name (strip folder prefix like "Device Control/")
        const variableId = metricName.includes("/")
          ? metricName.split("/").pop()!
          : metricName;
        const variable = variables.get(variableId);

        // Always forward command to NATS - even if variable not discovered yet
        // The PLC will receive it if subscribed to the subject
        const commandSubject = `${config.projectId}/${variableId}`;

        // Convert value based on known datatype or infer from metric value type
        let convertedValue: number | boolean | string | Record<string, unknown>;
        if (variable) {
          convertedValue = sparkplugToPLCValue(metric.value, variable.datatype);
        } else {
          // Infer type from the metric value
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
        nc.publish(commandSubject, String(convertedValue));

        // Update local state if variable is known
        if (variable) {
          variable.value = convertedValue;
          const deviceId = config.mqtt.edgeNode;
          // Use original metric name for synapse (it may include folder prefix)
          if (node.devices[deviceId]?.metrics[metricName]) {
            await setValue(node, metricName, convertedValue, deviceId);
          } else if (node.devices[deviceId]?.metrics[variableId]) {
            await setValue(node, variableId, convertedValue, deviceId);
          }
        }
      }
    }
  });

  // Continue processing NATS messages for ongoing updates
  // (the collection has already started in parallel and will complete)
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

        const { variableId, value, datatype, deadband, disableRBE } = data;

        // Update existing variable (or create if new)
        const variable = variables.get(variableId);
        if (variable) {
          variable.value = value as
            | number
            | boolean
            | string
            | Record<string, unknown>;
          // Update metadata if provided
          if (deadband !== undefined) variable.deadband = deadband;
          if (disableRBE !== undefined) variable.disableRBE = disableRBE;
        } else {
          // New variable discovered after initialization
          variables.set(variableId, {
            id: variableId,
            description: `${config.projectId}/${variableId}`,
            datatype,
            value: value as number | boolean | string | Record<string, unknown>,
            deadband,
            disableRBE,
          });
        }

        // Update synapse device metrics
        const deviceId = config.mqtt.edgeNode;
        if (!node.devices[deviceId].metrics[variableId]) {
          // Create new metric if it didn't exist at birth
          node.devices[deviceId].metrics[variableId] = createMetric(
            variableId,
            value,
            datatype,
            Date.now(),
            undefined, // scanRate
            deadband,
            disableRBE,
            "plc", // source
            "good", // quality
          );
          log.info(`New metric discovered: ${variableId}`);
        }

        // Use setValue to update and publish DDATA (handles RBE/deadband)
        await setValue(node, variableId, value, deviceId);

        log.debug(`Updated metric: ${variableId} = ${value} (${datatype})`);
      } catch (error) {
        log.error("Error processing NATS message:", error);
      }
    }
  })();

  log.info(`Synapse bridge initialized with ${variables.size} variables`);

  // Return manager interface
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
