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

export type BridgeConfig = {
  mqtt: MqttConfig;
  nats: NatsConfig;
  projectId: string;
  deviceId?: string; // Optional: if set, use DDATA instead of NDATA
};

/**
 * Load MQTT configuration from environment variables
 */
export function loadMqttConfig(): MqttConfig {
  const brokerUrl = Deno.env.get("MQTT_BROKER_URL");
  if (!brokerUrl) {
    throw new Error("MQTT_BROKER_URL environment variable is required");
  }

  return {
    brokerUrl,
    clientId: Deno.env.get("MQTT_CLIENT_ID") || `tentacle-mqtt-${Date.now()}`,
    groupId: Deno.env.get("MQTT_GROUP_ID") || "TentacleGroup",
    edgeNode: Deno.env.get("MQTT_EDGE_NODE") || "EdgeNode",
    username: Deno.env.get("MQTT_USERNAME"),
    password: Deno.env.get("MQTT_PASSWORD"),
    keepalive: parseInt(Deno.env.get("MQTT_KEEPALIVE") || "30"),
    tlsEnabled: brokerUrl.startsWith("mqtts://"),
    tlsCertPath: Deno.env.get("MQTT_TLS_CERT_PATH"),
    tlsKeyPath: Deno.env.get("MQTT_TLS_KEY_PATH"),
    tlsCaPath: Deno.env.get("MQTT_TLS_CA_PATH"),
  };
}

/**
 * Load NATS configuration from environment variables
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
 * Load bridge configuration from environment variables
 */
export function loadBridgeConfig(): BridgeConfig {
  const projectId = Deno.env.get("PROJECT_ID");
  if (!projectId) {
    throw new Error("PROJECT_ID environment variable is required");
  }

  return {
    mqtt: loadMqttConfig(),
    nats: loadNatsConfig(),
    projectId,
    deviceId: Deno.env.get("MQTT_DEVICE_ID"),
  };
}
