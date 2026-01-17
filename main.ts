/**
 * tentacle-mqtt - MQTT Sparkplug B bridge
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
 */

import { loadBridgeConfig } from "./types/config.ts";
import { setupSparkplugBridge } from "./mqtt.ts";

async function main() {
  try {
    console.log("=== tentacle-mqtt: MQTT Sparkplug B Bridge ===\n");

    // Load configuration from environment
    const config = loadBridgeConfig();

    console.log("Configuration:");
    console.log(`  Project ID: ${config.projectId}`);
    console.log(`  MQTT Broker: ${config.mqtt.brokerUrl}`);
    console.log(`  MQTT Group ID: ${config.mqtt.groupId}`);
    console.log(`  MQTT Edge Node: ${config.mqtt.edgeNode}`);
    console.log(`  NATS Servers: ${Array.isArray(config.nats.servers) ? config.nats.servers.join(", ") : config.nats.servers}`);
    console.log();

    // Setup the bridge
    const bridge = await setupSparkplugBridge(config);

    console.log("\nBridge running. Press Ctrl+C to stop.\n");

    // Handle graceful shutdown
    const shutdown = async () => {
      console.log("\n\nShutting down...");
      await bridge.disconnect();
      console.log("Goodbye!");
      Deno.exit(0);
    };

    Deno.addSignalListener("SIGINT", shutdown);
  } catch (error) {
    console.error("Fatal error:", error);
    Deno.exit(1);
  }
}

if (import.meta.main) {
  await main();
}
