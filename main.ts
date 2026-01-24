/**
 * tentacle-mqtt - MQTT Sparkplug B bridge
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
 */

import { createLogger, LogLevel } from "@joyautomation/coral";
import { loadBridgeConfig } from "./types/config.ts";
import { setupSparkplugBridge } from "./mqtt.ts";

const log = createLogger("mqtt-main", LogLevel.info);

async function main() {
  try {
    log.info("=== tentacle-mqtt: MQTT Sparkplug B Bridge ===");

    // Load configuration from environment
    const config = loadBridgeConfig();

    log.info("Configuration:");
    log.info(`  Project ID: ${config.projectId}`);
    log.info(`  MQTT Broker: ${config.mqtt.brokerUrl}`);
    log.info(`  MQTT Group ID: ${config.mqtt.groupId}`);
    log.info(`  MQTT Edge Node: ${config.mqtt.edgeNode}`);
    log.info(`  NATS Servers: ${Array.isArray(config.nats.servers) ? config.nats.servers.join(", ") : config.nats.servers}`);

    // Setup the bridge
    const bridge = await setupSparkplugBridge(config);

    log.info("Bridge running. Press Ctrl+C to stop.");

    // Handle graceful shutdown
    const shutdown = async () => {
      log.info("Shutting down...");
      await bridge.disconnect();
      log.info("Goodbye!");
      Deno.exit(0);
    };

    Deno.addSignalListener("SIGINT", shutdown);
  } catch (error) {
    log.error("Fatal error:", error);
    Deno.exit(1);
  }
}

if (import.meta.main) {
  await main();
}
