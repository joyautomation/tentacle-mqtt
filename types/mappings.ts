/**
 * Type mappings between NATS PLC variables and Synapse Sparkplug B metrics
 *
 * This module serves as middleware that converts from NATS schema types to Synapse types.
 * - Input: PLC variables with NATS deadband configuration
 * - Output: Synapse SparkplugMetric objects ready for MQTT publishing
 */

import type { SparkplugMetric } from "@joyautomation/synapse";

/**
 * Convert PLC datatype to Sparkplug B metric type
 * PLC types: number, boolean, string, udt
 * Sparkplug types: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float, double, boolean, string, datetime, text, uuid, dataset, bytes, file, template
 *
 * @returns A valid Sparkplug B type string ("double", "boolean", or "string")
 */
export function plcToSparkplugType(plcDatatype: string): string {
  switch (plcDatatype.toLowerCase()) {
    case "number":
      return "double"; // Use double for maximum precision
    case "boolean":
      return "boolean";
    case "string":
      return "string";
    case "udt":
      return "string"; // Serialize UDT as JSON string
    default:
      console.warn(`Unknown PLC datatype: ${plcDatatype}, defaulting to string`);
      return "string";
  }
}

/** Full metadata options for creating a metric with all available properties */
export type MetricMetadata = {
  /** PLC datatype (number, boolean, string, udt) */
  datatype: string;
  /** Source of the value (plc, mqtt, graphql, field, manual) */
  source?: string;
  /** Data quality (good, uncertain, bad) */
  quality?: string;
  /** RBE deadband configuration */
  deadband?: { value: number; maxTime?: number };
  /** Whether RBE checking is disabled */
  disableRBE?: boolean;
  /** Optional polling interval (ms) */
  scanRate?: number;
};

/**
 * Create a Synapse Sparkplug B metric from a PLC variable
 *
 * Converts PLC variable data and RBE configuration to a Synapse metric.
 * The deadband settings from NATS are applied to the metric so Synapse
 * can honor Report By Exception filtering during publishing.
 *
 * @param name - Metric name (usually variable ID)
 * @param value - Current value from PLC
 * @param plcDatatype - PLC datatype (number, boolean, string, udt)
 * @param timestamp - Optional timestamp (defaults to now)
 * @param scanRate - Optional polling interval (ms). If set, metric is polled; if not, published on change
 * @param deadband - Optional RBE deadband configuration from NATS
 * @param disableRBE - Optional flag to disable RBE checking for debugging
 * @param source - Optional source identifier (plc, mqtt, graphql, field, manual)
 * @param quality - Optional data quality (good, uncertain, bad)
 * @returns Synapse SparkplugMetric ready for MQTT publishing
 */
export function createMetric(
  name: string,
  value: unknown,
  plcDatatype: string,
  timestamp?: number,
  scanRate?: number,
  deadband?: { value: number; maxTime?: number },
  disableRBE?: boolean,
  source?: string,
  quality?: string,
  moduleId?: string,
  description?: string,
): SparkplugMetric {
  const sparkplugType = plcToSparkplugType(plcDatatype);
  let convertedValue: number | boolean | string | null = null;

  switch (sparkplugType) {
    case "double":
      convertedValue = typeof value === "number" ? value : parseFloat(String(value));
      break;
    case "boolean":
      if (typeof value === "boolean") {
        convertedValue = value;
      } else if (typeof value === "string") {
        convertedValue =
          value.toLowerCase() === "true" ||
          value === "1" ||
          value.toLowerCase() === "on" ||
          value.toLowerCase() === "yes";
      } else {
        convertedValue = Boolean(value);
      }
      break;
    case "string":
      if (typeof value === "string") {
        convertedValue = value;
      } else if (typeof value === "object" && value !== null) {
        // For UDT types, stringify the object
        convertedValue = JSON.stringify(value);
      } else {
        convertedValue = String(value);
      }
      break;
  }

  const metric: SparkplugMetric = {
    name,
    value: convertedValue,
    type: sparkplugType as never, // Cast string to Synapse's TypeStr literal union
    timestamp,
  };

  // Apply optional configuration
  if (scanRate !== undefined) {
    metric.scanRate = scanRate;
  }
  if (deadband) {
    metric.deadband = deadband;
  }
  if (disableRBE) {
    (metric as any).disableRBE = disableRBE;
  }

  // Store metadata as Sparkplug B metric properties
  metric.properties = {
    datatype: {
      value: plcDatatype,
      type: "String",
    },
  };

  // Add optional metadata properties if provided
  if (source) {
    metric.properties.source = {
      value: source,
      type: "String",
    };
  }
  if (quality) {
    metric.properties.quality = {
      value: quality,
      type: "String",
    };
  }
  if (deadband) {
    metric.properties.deadbandValue = {
      value: deadband.value,
      type: "Double",
    };
    if (deadband.maxTime !== undefined) {
      metric.properties.deadbandMaxTime = {
        value: deadband.maxTime,
        type: "Int32",
      };
    }
  }
  if (moduleId) {
    metric.properties.moduleId = {
      value: moduleId,
      type: "String",
    };
  }
  if (description) {
    metric.properties.description = {
      value: description,
      type: "String",
    };
  }

  return metric;
}

/**
 * Convert Sparkplug metric value to PLC type
 *
 * Used when processing MQTT commands (NCMD/DCMD) to convert values
 * back to PLC types before writing to the PLC system.
 */
export function sparkplugToPLCValue(
  value: unknown,
  targetType: string,
): number | boolean | string | Record<string, unknown> {
  switch (targetType.toLowerCase()) {
    case "number":
      return typeof value === "number" ? value : parseFloat(String(value));
    case "boolean":
      if (typeof value === "boolean") return value;
      if (typeof value === "string") {
        return (
          value.toLowerCase() === "true" ||
          value === "1" ||
          value.toLowerCase() === "on" ||
          value.toLowerCase() === "yes"
        );
      }
      return Boolean(value);
    case "string":
      return String(value);
    case "udt":
      if (typeof value === "string") {
        try {
          return JSON.parse(value) as Record<string, unknown>;
        } catch {
          return value;
        }
      }
      return value as Record<string, unknown>;
    default:
      return String(value);
  }
}
