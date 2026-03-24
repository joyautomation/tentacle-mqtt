/**
 * MQTT/Sparkplug B configuration for tentacle-mqtt
 */

export type MqttConfig = {
  brokerUrl: string; // mqtt:// or mqtts://
  clientId: string;
  groupId: string;
  edgeNode: string;
  username?: string;
  password?: string;
  keepalive?: number; // seconds, default 30
  tlsEnabled?: boolean;
  tlsCertPath?: string;
  tlsKeyPath?: string;
  tlsCaPath?: string;
};

export type NatsConfig = {
  servers: string | string[];
  user?: string;
  pass?: string;
  token?: string;
};

export type StoreForwardConfig = {
  /** Primary host ID to monitor via STATE topic. If not set, store & forward is disabled. */
  primaryHostId?: string;
  /** Max records to buffer (default 10000) */
  maxRecords: number;
  /** Max buffer size in bytes (default 50MB) */
  maxBytes: number;
  /** Records per second to drain during backfill (default 100) */
  drainRate: number;
};

export type BridgeConfig = {
  mqtt: MqttConfig;
  nats: NatsConfig;
  deviceId?: string; // Optional: if set, use DDATA instead of NDATA
  /** When true (default), UDT metrics with a template definition are published as
   *  Sparkplug B Template Instances. When false, UDTs are flattened into individual
   *  string/number/boolean metrics (old behaviour). Env: MQTT_USE_TEMPLATES */
  useTemplates: boolean;
  /** Store & Forward configuration */
  storeForward: StoreForwardConfig;
};

import type { ConfigManager, ConfigSchema } from "../nats-config.ts";
import { createConfigManager } from "../nats-config.ts";
import type { NatsConnection } from "@nats-io/transport-deno";

/**
 * MQTT config schema — defines all configurable fields with env var mappings.
 * NATS connection fields are envOnly (needed before KV is available).
 */
export const mqttConfigSchema = {
  // NATS connection — env only (chicken-and-egg: need NATS to read KV)
  natsServers: { envVar: "NATS_SERVERS", default: "localhost:4222", envOnly: true, description: "NATS server URL(s), comma-separated" },
  natsUser: { envVar: "NATS_USER", envOnly: true, description: "NATS username" },
  natsPass: { envVar: "NATS_PASS", envOnly: true, description: "NATS password" },
  natsToken: { envVar: "NATS_TOKEN", envOnly: true, description: "NATS token" },

  // MQTT broker
  brokerUrl: { envVar: "MQTT_BROKER_URL", required: true, description: "MQTT broker URL (mqtt:// or mqtts://)" },
  clientId: { envVar: "MQTT_CLIENT_ID", default: "tentacle-mqtt", description: "MQTT client ID base (random suffix appended)" },
  groupId: { envVar: "MQTT_GROUP_ID", default: "TentacleGroup", description: "Sparkplug B group ID" },
  edgeNode: { envVar: "MQTT_EDGE_NODE", default: "EdgeNode", description: "Sparkplug B edge node name" },
  deviceId: { envVar: "MQTT_DEVICE_ID", description: "Sparkplug B device ID (if set, uses DDATA)" },
  username: { envVar: "MQTT_USERNAME", description: "MQTT broker username" },
  password: { envVar: "MQTT_PASSWORD", description: "MQTT broker password" },
  keepalive: { envVar: "MQTT_KEEPALIVE", default: "30", type: "number" as const, description: "Keep-alive interval (seconds)" },

  // TLS
  tlsCertPath: { envVar: "MQTT_TLS_CERT_PATH", description: "Client certificate path (PEM)" },
  tlsKeyPath: { envVar: "MQTT_TLS_KEY_PATH", description: "Client private key path (PEM)" },
  tlsCaPath: { envVar: "MQTT_TLS_CA_PATH", description: "CA certificate path (PEM)" },

  // Sparkplug
  useTemplates: { envVar: "MQTT_USE_TEMPLATES", default: "true", type: "boolean" as const, description: "Use Sparkplug B templates for UDTs" },

  // Store & Forward
  primaryHostId: { envVar: "MQTT_PRIMARY_HOST_ID", description: "Primary host ID for store-forward STATE monitoring" },
  sfMaxRecords: { envVar: "MQTT_SF_MAX_RECORDS", default: "10000", type: "number" as const, description: "Max buffered records" },
  sfMaxMB: { envVar: "MQTT_SF_MAX_MB", default: "50", type: "number" as const, description: "Max buffer size (MB)" },
  sfDrainRate: { envVar: "MQTT_SF_DRAIN_RATE", default: "100", type: "number" as const, description: "Drain rate (records/sec)" },
} satisfies ConfigSchema;

export type MqttConfigManager = ConfigManager<typeof mqttConfigSchema>;

/**
 * Load NATS config from env vars (needed before KV is available)
 */
export function loadNatsConfig(): NatsConfig {
  const servers = Deno.env.get("NATS_SERVERS") || "localhost:4222";
  return {
    servers: servers.split(",").map((s) => s.trim()),
    user: Deno.env.get("NATS_USER"),
    pass: Deno.env.get("NATS_PASS"),
    token: Deno.env.get("NATS_TOKEN"),
  };
}

/**
 * Create the MQTT config manager backed by NATS KV.
 * Call after NATS connection is established.
 */
export async function createMqttConfigManager(nc: NatsConnection): Promise<MqttConfigManager> {
  return await createConfigManager(nc, "mqtt", mqttConfigSchema);
}

/**
 * Build a BridgeConfig from the config manager values.
 * This bridges the new config system with the existing BridgeConfig type.
 */
export function buildBridgeConfig(cfg: MqttConfigManager, nats: NatsConfig): BridgeConfig {
  const brokerUrl = cfg.get("brokerUrl") as string;
  const clientIdBase = cfg.get("clientId") as string || "tentacle-mqtt";

  return {
    mqtt: {
      brokerUrl,
      clientId: `${clientIdBase}-${crypto.randomUUID().slice(0, 8)}`,
      groupId: cfg.get("groupId") as string || "TentacleGroup",
      edgeNode: cfg.get("edgeNode") as string || "EdgeNode",
      username: cfg.get("username") as string | undefined,
      password: cfg.get("password") as string | undefined,
      keepalive: cfg.get("keepalive") as number ?? 30,
      tlsEnabled: brokerUrl.startsWith("mqtts://"),
      tlsCertPath: cfg.get("tlsCertPath") as string | undefined,
      tlsKeyPath: cfg.get("tlsKeyPath") as string | undefined,
      tlsCaPath: cfg.get("tlsCaPath") as string | undefined,
    },
    nats,
    deviceId: cfg.get("deviceId") as string | undefined,
    useTemplates: cfg.get("useTemplates") as boolean ?? true,
    storeForward: {
      primaryHostId: cfg.get("primaryHostId") as string | undefined,
      maxRecords: cfg.get("sfMaxRecords") as number ?? 10000,
      maxBytes: ((cfg.get("sfMaxMB") as number) ?? 50) * 1024 * 1024,
      drainRate: cfg.get("sfDrainRate") as number ?? 100,
    },
  };
}

/**
 * Legacy: Load bridge configuration from environment variables only (no KV).
 * Use buildBridgeConfig + createMqttConfigManager for KV-backed config.
 */
export function loadBridgeConfig(): BridgeConfig {
  const brokerUrl = Deno.env.get("MQTT_BROKER_URL");
  if (!brokerUrl) {
    throw new Error("MQTT_BROKER_URL environment variable is required");
  }
  const useTemplatesEnv = Deno.env.get("MQTT_USE_TEMPLATES");
  return {
    mqtt: {
      brokerUrl,
      clientId: `${Deno.env.get("MQTT_CLIENT_ID") || "tentacle-mqtt"}-${crypto.randomUUID().slice(0, 8)}`,
      groupId: Deno.env.get("MQTT_GROUP_ID") || "TentacleGroup",
      edgeNode: Deno.env.get("MQTT_EDGE_NODE") || "EdgeNode",
      username: Deno.env.get("MQTT_USERNAME"),
      password: Deno.env.get("MQTT_PASSWORD"),
      keepalive: parseInt(Deno.env.get("MQTT_KEEPALIVE") || "30"),
      tlsEnabled: brokerUrl.startsWith("mqtts://"),
      tlsCertPath: Deno.env.get("MQTT_TLS_CERT_PATH"),
      tlsKeyPath: Deno.env.get("MQTT_TLS_KEY_PATH"),
      tlsCaPath: Deno.env.get("MQTT_TLS_CA_PATH"),
    },
    nats: loadNatsConfig(),
    deviceId: Deno.env.get("MQTT_DEVICE_ID"),
    useTemplates: useTemplatesEnv !== undefined ? useTemplatesEnv.toLowerCase() !== "false" : true,
    storeForward: {
      primaryHostId: Deno.env.get("MQTT_PRIMARY_HOST_ID"),
      maxRecords: parseInt(Deno.env.get("MQTT_SF_MAX_RECORDS") || "10000", 10),
      maxBytes: parseInt(Deno.env.get("MQTT_SF_MAX_MB") || "50", 10) * 1024 * 1024,
      drainRate: parseInt(Deno.env.get("MQTT_SF_DRAIN_RATE") || "100", 10),
    },
  };
}
