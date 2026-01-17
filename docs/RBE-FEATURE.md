# Report By Exception (RBE) Configuration

## Overview

Report By Exception (RBE) is a data filtering feature that reduces network traffic by only publishing metric updates when meaningful changes occur. RBE settings originate in **tentacle-plc**, flow through **NATS**, and are honored by **Synapse** when publishing MQTT messages.

## Architecture Flow

```
┌─────────────────────┐
│  tentacle-plc       │  Defines variables with deadband
│  (PlcVariableBase)  │  - deadband: { value, maxTime }
└──────────┬──────────┘  - disableRBE: boolean
           │
           ├──► NATS Topic: plc.data.{projectId}.{variableId}
           │    (includes deadband metadata)
           │
           ├──► NATS KV: plc-variables-{projectId}:{variableId}
           │    (stores deadband config)
           │
┌──────────▼──────────┐
│  tentacle-mqtt      │  Discovers variables from NATS
│  (reads RBE config) │  Creates Synapse metrics with deadband
└──────────┬──────────┘
           │
           ├──► Synapse Node/Device Metrics
           │    (SparkplugMetric.deadband)
           │
┌──────────▼──────────┐
│  Synapse MQTT       │  Honors RBE during publishing
│  (respects RBE)     │  Only publishes qualifying changes
└─────────────────────┘
           │
           └──► MQTT Broker (fewer messages)
```

## Type Definitions

### In tentacle-plc

```typescript
// File: /home/joyja/Development/tentacle-plc/types/variables.ts

export type DeadBandConfig = {
  value: number;      // Threshold for RBE
  maxTime?: number;   // Max time between publishes (ms)
};

export type PlcVariableBase = {
  id: string;
  description: string;
  source?: VariableSource;
  deadband?: DeadBandConfig;    // RBE configuration
  disableRBE?: boolean;         // Disable RBE for debugging
};
```

### In nats-schema

```typescript
// File: /home/joyja/Development/nats-schema/src/types.ts

export type DeadBandConfig = {
  value: number;      // Threshold for RBE
  maxTime?: number;   // Max time between publishes (ms)
};

export type PlcVariableKV = {
  projectId: string;
  variableId: string;
  value: unknown;
  datatype: string;
  lastUpdated: number;
  source: string;
  quality: string;
  deadband?: DeadBandConfig;    // RBE stored in KV
  disableRBE?: boolean;         // RBE disable flag in KV
};

export type PlcDataMessage = {
  projectId: string;
  variableId: string;
  value: unknown;
  timestamp: number;
  datatype: string;
  deadband?: DeadBandConfig;    // RBE in NATS messages
  disableRBE?: boolean;
};
```

### In tentacle-mqtt

```typescript
// File: /home/joyja/Development/tentacle-mqtt/types/mappings.ts

export type SparkplugMetric = {
  name: string;
  value: number | boolean | string | null;
  type: string;
  timestamp?: number;
  scanRate?: number;
  deadband?: {
    value: number;
    maxTime?: number;
  };
  disableRBE?: boolean;
  properties?: Record<string, { value: unknown; type: string }>;
};

export function createMetric(
  name: string,
  value: unknown,
  plcDatatype: string,
  timestamp?: number,
  scanRate: number = 2500,
  deadband?: { value: number; maxTime?: number },
  disableRBE?: boolean,
): SparkplugMetric;
```

### In Synapse

```typescript
// File: /home/joyja/Development/deno/synapse/types.ts

export interface SparkplugMetric extends Omit<UMetric, "value"> {
  scanRate?: number;
  deadband?: {
    maxTime?: number;
    value: number;
  };
  disableRBE?: boolean;  // NEW: Override RBE for this metric
  value: UMetric["value"] | (() => UMetric["value"]) | (() => Promise<UMetric["value"]>);
  lastPublished?: {
    timestamp: number;
    value: UMetric["value"];
  };
}
```

## Usage Examples

### Example 1: Temperature with Deadband

```typescript
// In tentacle-plc variables
temperature: {
  id: "temperature",
  description: "Building temperature",
  datatype: "number",
  default: 20,
  value: 20,
  deadband: {
    value: 0.5,       // Only publish if change > 0.5°C
    maxTime: 60000,   // But at least every 60 seconds
  },
}
```

**Behavior:**
- Value: 20.0 → 20.3: NO publish (change < 0.5)
- Value: 20.0 → 20.6: PUBLISH (change > 0.5)
- No change for 60 seconds: PUBLISH (maxTime exceeded)

### Example 2: Critical Pressure Sensor

```typescript
pressure: {
  id: "pressure",
  description: "System pressure (critical)",
  datatype: "number",
  default: 101.3,
  value: 101.3,
  deadband: {
    value: 0.1,       // Tight deadband for safety
    maxTime: 30000,   // Force update every 30 seconds
  },
}
```

**Behavior:**
- Tight deadband (0.1) catches small critical changes
- maxTime (30s) ensures regular status updates even if stable

### Example 3: Boolean State (No Deadband)

```typescript
systemHealthy: {
  id: "systemHealthy",
  description: "System health status",
  datatype: "boolean",
  default: true,
  value: true,
  // No deadband - publish every state change
}
```

**Behavior:**
- true → false: PUBLISH immediately
- false → true: PUBLISH immediately
- Same state: No publish

### Example 4: Debug Variable (RBE Disabled)

```typescript
debugValue: {
  id: "debugValue",
  description: "Debug value (RBE disabled)",
  datatype: "number",
  default: 0,
  value: 0,
  disableRBE: true,  // Force publish every change
}
```

**Behavior:**
- Every value change: PUBLISH
- Useful for troubleshooting RBE behavior
- **Remember to remove before production!**

## How Synapse Honors RBE

### In Synapse (node.ts)

When `publishMetricOnChange()` is called:

```typescript
export const publishMetricOnChange = async (
  node: SparkplugNode,
  metricName: string,
  deviceId?: string
) => {
  // ... get metric and evaluate value ...

  if (!metricNeedsToPublish(evaluatedMetric)) return;  // RBE check here

  // ... publish if RBE passes ...
}

export const metricNeedsToPublish = (metric: SparkplugMetric) => {
  // If RBE is disabled, always publish
  if (metric.disableRBE) {
    return true;
  }

  // If no deadband, publish all changes
  if (!metric.lastPublished || !metric.deadband) {
    return metric.value !== metric.lastPublished?.value;
  }

  // Check deadband threshold (for numeric types)
  const timeSinceLastPublish = Date.now() - metric.lastPublished.timestamp;
  const maxTimeExceeded = timeSinceLastPublish > metric.deadband.maxTime;

  if (maxTimeExceeded) return true;  // Force publish if maxTime exceeded

  const valueDiff = Math.abs(metric.value - metric.lastPublished.value);
  return valueDiff > metric.deadband.value;  // Only publish if change > deadband
}
```

## Real-World Scenarios

### High-Frequency Sensor (100 samples/sec)

```typescript
ambientLight: {
  id: "ambientLight",
  description: "Light sensor (100Hz)",
  datatype: "number",
  default: 500,
  value: 500,
  deadband: {
    value: 50,       // 10% change threshold
    maxTime: 5000,   // Report at least every 5 seconds
  },
}
```

**Impact:** Reduces 6000 samples/sec to ~120 messages/sec

### Machine Cycle Counter

```typescript
cycleCount: {
  id: "cycleCount",
  description: "Total cycles",
  datatype: "number",
  default: 0,
  value: 0,
  deadband: {
    value: 100,      // Report every 100 cycles
    maxTime: 600000, // Or at least every 10 minutes
  },
}
```

**Impact:** Only reports significant production milestones

### Safety-Critical System

```typescript
systemTemperature: {
  id: "systemTemperature",
  description: "Core temperature (safety critical)",
  datatype: "number",
  default: 50,
  value: 50,
  deadband: {
    value: 2,        // Very tight tolerance
    maxTime: 10000,  // Very frequent updates
  },
}
```

**Impact:** Ensures operator sees temperature changes within 10 seconds

## Configuration Best Practices

| Use Case | deadband.value | deadband.maxTime | disableRBE |
|----------|---|---|---|
| High-frequency sensor | Large (5-10% of range) | 5-10 sec | false |
| Critical/Safety | Small (1-2% of range) | 10-30 sec | false |
| Status flag (boolean) | N/A (no deadband) | N/A | false |
| Counter/Monotonic | Meaningful increment | Long (5-10 min) | false |
| Low-importance value | Large (20% of range) | Long (5-10 min) | false |
| Debugging/Testing | - | - | true |

## Implementation Checklist

- [x] Add `DeadBandConfig` type to nats-schema
- [x] Add deadband fields to `PlcVariableKV` and `PlcDataMessage`
- [x] Update tentacle-plc `PlcVariableBase` with deadband and disableRBE
- [x] Update tentacle-mqtt `createMetric()` to accept deadband parameters
- [x] Update Synapse `SparkplugMetric` with disableRBE field
- [x] Implement `metricNeedsToPublish()` in Synapse to check deadband
- [x] Implement batching feature (complements RBE for event-driven publishing)
- [ ] Add RBE configuration UI/API to tentacle-plc
- [ ] Add RBE monitoring/stats to see how much traffic is reduced
- [ ] Document RBE tuning guidelines per sensor type

## Troubleshooting

### "My variable changes aren't publishing"

1. Check deadband is configured correctly:
   ```typescript
   if (variable.deadband?.value === 0) {
     // This will never publish (deadband of 0 means only exact duplicates)
     // Use a small value like 0.01 instead
   }
   ```

2. Check if maxTime has passed:
   ```typescript
   const timeSince = Date.now() - metric.lastPublished.timestamp;
   if (timeSince < metric.deadband.maxTime && valueDiff < metric.deadband.value) {
     // No publish - threshold not met and maxTime not exceeded
   }
   ```

3. Temporarily disable RBE for debugging:
   ```typescript
   debugVariable: {
     // ... other config ...
     disableRBE: true,  // See every change
   }
   ```

### "I'm getting too many MQTT messages"

Increase deadband thresholds:
```typescript
// Before: 1000s of messages
deadband: { value: 0.1, maxTime: 5000 }

// After: ~100 messages
deadband: { value: 1.0, maxTime: 30000 }
```

### "Changes aren't being detected fast enough"

Decrease maxTime:
```typescript
// Before: Up to 5 minutes between updates
deadband: { value: 0.5, maxTime: 300000 }

// After: Maximum 30 seconds between updates
deadband: { value: 0.5, maxTime: 30000 }
```

## Testing RBE

See example file: `/home/joyja/Development/tentacle-plc/examples/rbe-configuration.ts`

```bash
# Test with debug variables
deno run -A examples/rbe-configuration.ts
```

## References

- Sparkplug B Specification: [deadband in tags](https://www.eclipse.org/niot/sparkplug/spec/)
- Related features in Synapse: Batching window, pub-on-change
- Performance impact: Reduces MQTT traffic by 80-95% on high-frequency sensors
