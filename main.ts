/**
 * tentacle-mqtt - MQTT Sparkplug B bridge
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
 */

import { createLogger, LogLevel, type Log } from "@joyautomation/coral";
import { loadBridgeConfig } from "./types/config.ts";
import { setupSparkplugBridge } from "./mqtt.ts";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import type { ServiceHeartbeat, ServiceLogEntry } from "@joyautomation/nats-schema";
import type { NatsConnection } from "@nats-io/transport-deno";

let log: Log = createLogger("mqtt-main", LogLevel.info);

function createNatsLogger(
  coralLog: Log,
  nc: NatsConnection,
  serviceType: string,
  moduleId: string,
  loggerName: string,
): Log {
  const subject = `service.logs.${serviceType}.${moduleId}`;
  const encoder = new TextEncoder();
  const formatArgs = (args: unknown[]): string =>
    args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ");
  const publish = (level: string, msg: string, ...args: unknown[]) => {
    try {
      const message = args.length > 0 ? `${msg} ${formatArgs(args)}` : msg;
      const entry: ServiceLogEntry = {
        timestamp: Date.now(),
        level: level as ServiceLogEntry["level"],
        message,
        serviceType,
        moduleId,
        logger: loggerName,
      };
      nc.publish(subject, encoder.encode(JSON.stringify(entry)));
    } catch { /* never break the service for logging */ }
  };
  return {
    info: (m: string, ...a: unknown[]) => { coralLog.info(m, ...a); publish("info", m, ...a); },
    warn: (m: string, ...a: unknown[]) => { coralLog.warn(m, ...a); publish("warn", m, ...a); },
    error: (m: string, ...a: unknown[]) => { coralLog.error(m, ...a); publish("error", m, ...a); },
    debug: (m: string, ...a: unknown[]) => { coralLog.debug(m, ...a); publish("debug", m, ...a); },
  } as Log;
}

async function main() {
  try {
    log.info("=== tentacle-mqtt: MQTT Sparkplug B Bridge ===");

    // Load configuration from environment
    const config = loadBridgeConfig();

    log.info("Configuration:");
    log.info(`  MQTT Broker: ${config.mqtt.brokerUrl}`);
    log.info(`  MQTT Group ID: ${config.mqtt.groupId}`);
    log.info(`  MQTT Edge Node: ${config.mqtt.edgeNode}`);
    log.info(`  NATS Servers: ${Array.isArray(config.nats.servers) ? config.nats.servers.join(", ") : config.nats.servers}`);

    // Setup the bridge
    const bridge = await setupSparkplugBridge(config);

    // Enable NATS log streaming
    log = createNatsLogger(log, bridge.natsConnection, "mqtt", "mqtt", "mqtt-main");

    // Heartbeat publishing for service discovery
    const js = jetstream(bridge.natsConnection);
    const kvm = new Kvm(js);
    const heartbeatsKv = await kvm.create("service_heartbeats", {
      history: 1,
      ttl: 60 * 1000,
    });
    const heartbeatKey = "mqtt";
    const startedAt = Date.now();

    const publishHeartbeat = async () => {
      const heartbeat: ServiceHeartbeat = {
        serviceType: "mqtt",
        moduleId: "mqtt",
        lastSeen: Date.now(),
        startedAt,
        metadata: {
          brokerUrl: config.mqtt.brokerUrl,
          edgeNode: config.mqtt.edgeNode,
        },
      };
      try {
        const encoder = new TextEncoder();
        await heartbeatsKv.put(heartbeatKey, encoder.encode(JSON.stringify(heartbeat)));
      } catch (err) {
        log.warn(`Failed to publish heartbeat: ${err}`);
      }
    };

    await publishHeartbeat();
    log.info("Service heartbeat started (moduleId: mqtt)");
    const heartbeatInterval = setInterval(publishHeartbeat, 10000);

    log.info("Bridge running. Press Ctrl+C to stop.");

    // Handle graceful shutdown
    const shutdown = async () => {
      log.info("Shutting down...");
      clearInterval(heartbeatInterval);
      try {
        await heartbeatsKv.delete(heartbeatKey);
      } catch {
        // May already be expired
      }
      await bridge.disconnect();
      log.info("Goodbye!");
      Deno.exit(0);
    };

    Deno.addSignalListener("SIGINT", shutdown);
    Deno.addSignalListener("SIGTERM", shutdown);

    // Listen for NATS shutdown command from graphql
    const shutdownSub = bridge.natsConnection.subscribe("mqtt.shutdown");
    (async () => {
      for await (const _msg of shutdownSub) {
        log.info("Received shutdown command via NATS");
        await shutdown();
        break;
      }
    })();
  } catch (error) {
    log.error("Fatal error:", error);
    Deno.exit(1);
  }
}

if (import.meta.main) {
  await main();
}
