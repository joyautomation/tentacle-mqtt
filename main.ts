/**
 * tentacle-mqtt - MQTT Sparkplug B bridge
 * Bridges PLC variables from NATS to MQTT using Sparkplug B specification
 */

import { createLogger, LogLevel, type Log } from "@joyautomation/coral";
import { loadNatsConfig, createMqttConfigManager, buildBridgeConfig } from "./types/config.ts";
import { setupSparkplugBridge, setBridgeLogger } from "./mqtt.ts";
import { connect } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import type { ServiceHeartbeat, ServiceLogEntry, ServiceEnabledKV } from "@joyautomation/nats-schema";
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

    // Load NATS config from env (needed before KV is available)
    const natsConfig = loadNatsConfig();
    log.info(`NATS Servers: ${Array.isArray(natsConfig.servers) ? natsConfig.servers.join(", ") : natsConfig.servers}`);

    // Connect to NATS for config watching (kept alive for the lifetime of the process)
    const configNc = await connect({
      servers: natsConfig.servers,
      user: natsConfig.user,
      pass: natsConfig.pass,
      token: natsConfig.token,
    });
    log.info("Connected to NATS for config");

    // Load config from NATS KV (falls back to env vars on first boot)
    const configManager = await createMqttConfigManager(configNc);

    const heartbeatKey = "mqtt";
    const startedAt = Date.now();

    // ── Bridge lifecycle ──────────────────────────────────────────────────
    let bridge: Awaited<ReturnType<typeof setupSparkplugBridge>> | null = null;
    let heartbeatInterval: ReturnType<typeof setInterval> | null = null;
    // deno-lint-ignore no-explicit-any
    let enabledWatcher: any = null;
    let shutdownSub: ReturnType<NatsConnection["subscribe"]> | null = null;

    async function startBridge() {
      const config = buildBridgeConfig(configManager, natsConfig);

      log.info("Configuration (from NATS KV):");
      log.info(`  MQTT Broker: ${config.mqtt.brokerUrl}`);
      log.info(`  MQTT Group ID: ${config.mqtt.groupId}`);
      log.info(`  MQTT Edge Node: ${config.mqtt.edgeNode}`);
      log.info(`  Device ID: ${config.deviceId || "(none)"}`);
      log.info(`  Use Templates: ${config.useTemplates}`);
      log.info(`  Primary Host: ${config.storeForward.primaryHostId || "(none)"}`);

      bridge = await setupSparkplugBridge(config);

      // Enable NATS log streaming for both main and bridge loggers
      log = createNatsLogger(log, bridge.natsConnection, "mqtt", "mqtt", "mqtt-main");
      setBridgeLogger(createNatsLogger(
        createLogger("mqtt-bridge", LogLevel.info),
        bridge.natsConnection, "mqtt", "mqtt", "mqtt-bridge",
      ));

      // Heartbeat publishing for service discovery
      const js = jetstream(bridge.natsConnection);
      const kvm = new Kvm(js);
      const heartbeatsKv = await kvm.create("service_heartbeats", {
        history: 1,
        ttl: 60 * 1000,
      });

      // ── Service enabled/disabled state ────────────────────────────────
      const enabledKv = await kvm.create("service_enabled", {
        history: 1,
        ttl: 0,
      });

      // Check initial enabled state
      try {
        const entry = await enabledKv.get(heartbeatKey);
        if (entry?.value) {
          const state = JSON.parse(new TextDecoder().decode(entry.value)) as ServiceEnabledKV;
          bridge.setEnabled(state.enabled);
          log.info(`Initial enabled state: ${state.enabled}`);
        }
      } catch {
        // Key doesn't exist = enabled by default
      }

      // Watch for enabled state changes
      enabledWatcher = await enabledKv.watch({ key: heartbeatKey });
      const currentWatcher = enabledWatcher;
      (async () => {
        for await (const entry of currentWatcher) {
          if (entry === null) continue;
          if (entry.operation === "DEL" || entry.operation === "PURGE") {
            bridge?.setEnabled(true);
            continue;
          }
          if (entry.value) {
            try {
              const state = JSON.parse(new TextDecoder().decode(entry.value)) as ServiceEnabledKV;
              bridge?.setEnabled(state.enabled);
            } catch {
              // Invalid data — ignore
            }
          }
        }
      })();

      const publishHeartbeat = async () => {
        const heartbeat: ServiceHeartbeat = {
          serviceType: "mqtt",
          moduleId: "mqtt",
          lastSeen: Date.now(),
          startedAt,
          metadata: {
            brokerUrl: config.mqtt.brokerUrl,
            clientId: config.mqtt.clientId,
            groupId: config.mqtt.groupId,
            edgeNode: config.mqtt.edgeNode,
            username: config.mqtt.username ?? "",
            password: config.mqtt.password ? "••••••••" : "",
            keepalive: String(config.mqtt.keepalive ?? 30),
            tlsEnabled: String(config.mqtt.tlsEnabled ?? false),
            useTemplates: String(config.useTemplates),
            deviceId: config.deviceId ?? "",
            enabled: String(bridge?.enabled ?? true),
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
      heartbeatInterval = setInterval(publishHeartbeat, 10000);

      // Listen for NATS shutdown command from graphql
      shutdownSub = bridge.natsConnection.subscribe("mqtt.shutdown");
      const currentShutdownSub = shutdownSub;
      (async () => {
        for await (const _msg of currentShutdownSub) {
          log.info("Received shutdown command via NATS");
          await shutdown();
          break;
        }
      })();

      log.info("Bridge running.");
    }

    async function stopBridge() {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
      }
      if (enabledWatcher) {
        try { enabledWatcher.stop(); } catch { /* already stopped */ }
        enabledWatcher = null;
      }
      if (shutdownSub) {
        shutdownSub.unsubscribe();
        shutdownSub = null;
      }
      if (bridge) {
        await bridge.disconnect();
        bridge = null;
      }
    }

    // ── Shutdown (defined before startBridge so it can be referenced) ────
    let restartTimer: ReturnType<typeof setTimeout> | null = null;
    let restarting = false;

    const shutdown = async () => {
      log.info("Shutting down...");
      if (restartTimer) clearTimeout(restartTimer);
      configManager.destroy();
      await stopBridge();
      await configNc.close();
      log.info("Goodbye!");
      Deno.exit(0);
    };

    // ── Config change watcher (debounced restart) ─────────────────────────
    configManager.onChange((key, value) => {
      log.info(`Config changed: ${key} = ${key === "password" ? "••••••••" : value}`);
      if (restarting) return;
      // Debounce — "Save All" fires one mutation per field
      if (restartTimer) clearTimeout(restartTimer);
      restartTimer = setTimeout(async () => {
        restartTimer = null;
        restarting = true;
        try {
          log.info("Restarting bridge with updated config...");
          await stopBridge();
          await startBridge();
          log.info("Bridge restarted successfully.");
        } catch (err) {
          log.error("Failed to restart bridge:", err);
        } finally {
          restarting = false;
        }
      }, 2000);
    });

    // ── Initial start ─────────────────────────────────────────────────────
    await startBridge();

    Deno.addSignalListener("SIGINT", shutdown);
    Deno.addSignalListener("SIGTERM", shutdown);

    log.info("Press Ctrl+C to stop.");
  } catch (error) {
    log.error("Fatal error:", error);
    Deno.exit(1);
  }
}

if (import.meta.main) {
  await main();
}
