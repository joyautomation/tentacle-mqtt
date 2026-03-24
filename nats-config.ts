/**
 * NATS KV Config Framework
 *
 * Provides runtime-configurable settings backed by NATS KV.
 * Env vars serve as first-boot defaults — once written to KV,
 * the KV value takes precedence on subsequent boots.
 *
 * Usage:
 * ```typescript
 * const configManager = await createConfigManager(nc, "mqtt", {
 *   brokerUrl: { envVar: "MQTT_BROKER_URL", required: true },
 *   groupId: { envVar: "MQTT_GROUP_ID", default: "TentacleGroup" },
 *   keepalive: { envVar: "MQTT_KEEPALIVE", default: "30", type: "number" },
 *   useTemplates: { envVar: "MQTT_USE_TEMPLATES", default: "true", type: "boolean" },
 * });
 *
 * const brokerUrl = configManager.get("brokerUrl"); // string
 * configManager.onChange("brokerUrl", (newValue) => { ... });
 * ```
 */

import { Kvm } from "@nats-io/kv";
import { jetstream } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/transport-deno";

export type ConfigFieldType = "string" | "number" | "boolean";

export type ConfigFieldDef = {
  /** Environment variable name (used as KV key suffix and first-boot source) */
  envVar: string;
  /** Default value if neither KV nor env var is set */
  default?: string;
  /** Whether this field is required (throws on startup if missing) */
  required?: boolean;
  /** Value type for parsing (default: "string") */
  type?: ConfigFieldType;
  /** Human-readable description */
  description?: string;
  /** If true, this field stays env-only and is NOT stored in KV (e.g., NATS credentials) */
  envOnly?: boolean;
};

export type ConfigSchema = Record<string, ConfigFieldDef>;

export type ConfigValues<S extends ConfigSchema> = Record<keyof S, string | number | boolean | undefined>;

type ChangeListener = (key: string, value: string) => void;

export type ConfigManager<S extends ConfigSchema> = {
  /** Get a config value */
  get(key: keyof S & string): string | number | boolean | undefined;
  /** Get all config values */
  getAll(): Record<string, string | number | boolean | undefined>;
  /** Register a callback for when a config value changes at runtime */
  onChange(listener: ChangeListener): () => void;
  /** Get the raw string value for a key */
  getRaw(key: string): string | undefined;
  /** Get the full schema (for UI rendering) */
  getSchema(): S;
  /** Programmatically set a value (writes to KV) */
  set(key: string, value: string): Promise<void>;
  /** Stop watching for changes */
  destroy(): void;
};

const CONFIG_BUCKET = "tentacle_config";

/**
 * Create a config manager for a service module.
 *
 * @param nc - NATS connection
 * @param moduleId - Service identifier (e.g., "mqtt", "snmp", "history")
 * @param schema - Config field definitions
 * @returns ConfigManager instance
 */
export async function createConfigManager<S extends ConfigSchema>(
  nc: NatsConnection,
  moduleId: string,
  schema: S,
): Promise<ConfigManager<S>> {
  const js = jetstream(nc);
  const kvm = new Kvm(js);

  // Create or open the config KV bucket
  const kv = await kvm.create(CONFIG_BUCKET, {
    history: 5,
    ttl: 0, // No expiration
  });

  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  const values = new Map<string, string>();
  const listeners = new Set<ChangeListener>();

  // Load initial values: KV first, then env var fallback, then default
  for (const [key, def] of Object.entries(schema)) {
    const kvKey = `${moduleId}.${def.envVar}`;
    let value: string | undefined;

    if (!def.envOnly) {
      // Try KV first
      try {
        const entry = await kv.get(kvKey);
        if (entry?.value) {
          value = decoder.decode(entry.value);
        }
      } catch {
        // Key doesn't exist in KV
      }
    }

    // Fall back to env var
    if (value === undefined) {
      value = Deno.env.get(def.envVar) ?? def.default;
    }

    // Check required
    if (def.required && (value === undefined || value === "")) {
      throw new Error(`Required config '${key}' (${def.envVar}) is not set`);
    }

    if (value !== undefined) {
      values.set(key, value);

      // Write to KV if not already there (first-boot persistence)
      if (!def.envOnly) {
        try {
          const existing = await kv.get(kvKey);
          if (!existing?.value) {
            await kv.put(kvKey, encoder.encode(value));
          }
        } catch {
          // KV write failed — not fatal
        }
      }
    }
  }

  // Watch for runtime changes
  let watcher: Awaited<ReturnType<typeof kv.watch>> | null = null;
  try {
    watcher = await kv.watch({ key: `${moduleId}.>` });
    (async () => {
      for await (const entry of watcher!) {
        if (entry === null) continue;
        const kvKey = entry.key; // e.g., "mqtt:MQTT_BROKER_URL"
        const prefix = `${moduleId}.`;
        if (!kvKey.startsWith(prefix)) continue;

        const envVar = kvKey.slice(prefix.length);
        // Find the schema key for this envVar
        const schemaEntry = Object.entries(schema).find(([_, def]) => def.envVar === envVar);
        if (!schemaEntry) continue;

        const [schemaKey] = schemaEntry;
        const newValue = entry.value ? decoder.decode(entry.value) : undefined;

        if (newValue !== undefined && newValue !== values.get(schemaKey)) {
          values.set(schemaKey, newValue);
          for (const listener of listeners) {
            try {
              listener(schemaKey, newValue);
            } catch {
              // Don't break on listener errors
            }
          }
        }
      }
    })();
  } catch {
    // Watch failed — config changes won't be live
  }

  function parseValue(key: string, raw: string | undefined): string | number | boolean | undefined {
    if (raw === undefined) return undefined;
    const def = schema[key];
    if (!def) return raw;

    switch (def.type) {
      case "number":
        return Number(raw);
      case "boolean":
        return raw.toLowerCase() === "true" || raw === "1";
      default:
        return raw;
    }
  }

  return {
    get(key: keyof S & string): string | number | boolean | undefined {
      return parseValue(key, values.get(key));
    },

    getAll(): Record<string, string | number | boolean | undefined> {
      const result: Record<string, string | number | boolean | undefined> = {};
      for (const key of Object.keys(schema)) {
        result[key] = parseValue(key, values.get(key));
      }
      return result;
    },

    onChange(listener: ChangeListener): () => void {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },

    getRaw(key: string): string | undefined {
      return values.get(key);
    },

    getSchema(): S {
      return schema;
    },

    async set(key: string, value: string): Promise<void> {
      const def = schema[key];
      if (!def) throw new Error(`Unknown config key: ${key}`);
      if (def.envOnly) throw new Error(`Config key '${key}' is env-only and cannot be set at runtime`);

      const kvKey = `${moduleId}.${def.envVar}`;
      await kv.put(kvKey, encoder.encode(value));
      // Watcher will pick up the change and update values + notify listeners
    },

    destroy(): void {
      if (watcher) {
        try { watcher.stop(); } catch { /* ignore */ }
        watcher = null;
      }
      listeners.clear();
    },
  };
}
