/**
 * MQTT Sparkplug B bridge for tentacle-mqtt
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
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

/**
 * Setup MQTT Sparkplug B bridge
 * Subscribes to MQTT-enabled variables from NATS and bridges to Sparkplug B.
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

  // Subscribe to MQTT-specific topics (only MQTT-enabled variables)
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

  // Handle device commands (DCMD) from MQTT
  node.events?.on("dcmd", async (_topic: string, message: any) => {
    log.info("Received DCMD:", message);

    if (message.metrics) {
      for (const metric of message.metrics) {
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
        nc.publish(commandSubject, String(convertedValue));

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
  });

  // Process NATS messages - add new variables dynamically (triggers rebirth)
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

        const { variableId, value, deadband, disableRBE } = data;
        const deviceId = config.mqtt.edgeNode;

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
          if (deadband !== undefined) existing.deadband = deadband;
          if (disableRBE !== undefined) existing.disableRBE = disableRBE;
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

        // Add new metric to device if not exists
        const isNewMetric = !node.devices[deviceId].metrics[variableId];
        if (isNewMetric) {
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
          log.info(`New metric: ${variableId}, adding to device and triggering rebirth`);
          addMetrics(node, { [variableId]: newMetric }, deviceId);

          // Trigger device rebirth to include new metric in DBIRTH
          if (node.mqtt) {
            const device = node.devices[deviceId];
            const mqttConfig = {
              version: node.version || "spBv1.0",
              groupId: node.groupId,
              edgeNode: node.id,
            };
            const birthPayload = {
              timestamp: Date.now(),
              metrics: Object.values(device.metrics).map(m => ({
                ...m,
                timestamp: Date.now(),
              })),
            };
            publishDeviceBirth(node, birthPayload, mqttConfig, node.mqtt, deviceId);
            log.info(`Published DBIRTH for device ${deviceId} with ${Object.keys(device.metrics).length} metrics`);
          }
        }

        // Update value via setValue
        const result = await setValue(node, variableId, value, deviceId);

        // Manually publish DDATA since synapse's RBE may not trigger
        if (node.mqtt && !isNewMetric) {
          const metric = node.devices[deviceId]?.metrics[variableId];
          if (metric) {
            const mqttConfig = {
              version: node.version || "spBv1.0",
              groupId: node.groupId,
              edgeNode: node.id,
            };
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

  log.info("Bridge ready - variables will be added as they arrive from NATS");

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
