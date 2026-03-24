/**
 * Sparkplug B Store & Forward Buffer
 *
 * Buffers DDATA payloads in memory when the primary host is offline.
 * Drains (backfills) when the host comes back online, setting isHistorical
 * on all replayed metrics per the Sparkplug B specification.
 */

import { createLogger, LogLevel } from "@joyautomation/coral";

const log = createLogger("store-forward", LogLevel.info);

export type BufferedRecord = {
  /** Sparkplug MQTT topic (e.g., spBv1.0/group/DDATA/node/device) */
  topic: string;
  /** Encoded Sparkplug payload (protobuf) */
  payload: Uint8Array;
  /** When this record was buffered (Date.now() ms) */
  bufferedAt: number;
};

export type StoreForwardConfig = {
  /** Max number of records to buffer. Oldest evicted when full. */
  maxRecords: number;
  /** Max buffer size in bytes. Oldest evicted when exceeded. */
  maxBytes: number;
  /** Records per second to drain during backfill */
  drainRate: number;
};

export type StoreForwardState = {
  primaryHostId: string | null;
  primaryHostOnline: boolean;
  bufferedRecords: number;
  bufferSizeBytes: number;
  bufferCapacityRecords: number;
  bufferCapacityBytes: number;
  bufferUsedPercentRecords: number;
  bufferUsedPercentBytes: number;
  draining: boolean;
  drainProgress: number;
  drainRecordsRemaining: number;
  drainTotalRecords: number;
  drainEtaSeconds: number;
  drainStartedAt: number | null;
  totalBuffered: number;
  totalDrained: number;
  totalEvicted: number;
  publishRate: number;
};

export class StoreForwardBuffer {
  private records: BufferedRecord[] = [];
  private currentBytes = 0;
  private config: StoreForwardConfig;

  // Drain state
  private _draining = false;
  private _drainTotal = 0;
  private _drainDrained = 0;
  private _drainStartedAt: number | null = null;
  private _drainInterval: ReturnType<typeof setInterval> | null = null;

  // Stats
  private _totalBuffered = 0;
  private _totalDrained = 0;
  private _totalEvicted = 0;

  // Primary host state
  private _primaryHostId: string | null = null;
  private _primaryHostOnline = true; // assume online until we hear otherwise
  private _offlineDebounce: ReturnType<typeof setTimeout> | null = null;
  private _offlineDebounceMs = 2000; // wait 2s before committing to OFFLINE

  // Publish rate tracking (sliding window)
  private _publishTimestamps: number[] = [];
  private _rateWindowMs = 10_000; // 10 second window

  // Callbacks
  private _onPublish: ((topic: string, payload: Uint8Array, isHistorical: boolean) => void) | null = null;

  // Status timeline history (state samples for the last hour)
  private _timeline: Array<{ timestamp: number; state: "online" | "buffering" | "draining" }> = [];
  private _timelineInterval: ReturnType<typeof setInterval> | null = null;

  constructor(config: StoreForwardConfig) {
    this.config = config;

    // Sample service state every 10 seconds for timeline
    this._timelineInterval = setInterval(() => {
      const state = this._draining ? "draining" as const
        : !this._primaryHostOnline ? "buffering" as const
        : "online" as const;
      this._timeline.push({ timestamp: Date.now(), state });
      // Keep last hour of samples (360 samples at 10s interval)
      if (this._timeline.length > 360) {
        this._timeline.shift();
      }
    }, 10_000);
  }

  /**
   * Record that metrics were published to MQTT (for rate tracking)
   * @param metricCount - number of individual metrics in this publish
   */
  recordPublish(metricCount = 1): void {
    const now = Date.now();
    for (let i = 0; i < metricCount; i++) {
      this._publishTimestamps.push(now);
    }
  }

  /**
   * Get the current publish rate (metrics per second, averaged over sliding window)
   */
  getPublishRate(): number {
    const now = Date.now();
    const cutoff = now - this._rateWindowMs;
    // Trim old timestamps
    while (this._publishTimestamps.length > 0 && this._publishTimestamps[0] < cutoff) {
      this._publishTimestamps.shift();
    }
    if (this._publishTimestamps.length === 0) return 0;
    return this._publishTimestamps.length / (this._rateWindowMs / 1000);
  }

  /**
   * Set the publish callback used during drain
   */
  setPublishCallback(fn: (topic: string, payload: Uint8Array, isHistorical: boolean) => void): void {
    this._onPublish = fn;
  }

  /**
   * Set the primary host ID to monitor
   */
  setPrimaryHostId(id: string): void {
    this._primaryHostId = id;
    log.info(`Monitoring primary host: ${id}`);
  }

  get primaryHostId(): string | null {
    return this._primaryHostId;
  }

  get primaryHostOnline(): boolean {
    return this._primaryHostOnline;
  }

  /**
   * Handle a STATE message for a primary host.
   * OFFLINE is debounced to avoid false buffering from retained message races
   * (broker delivers retained OFFLINE then live ONLINE in rapid succession).
   */
  handleStateChange(hostId: string, online: boolean): void {
    if (!this._primaryHostId || hostId !== this._primaryHostId) return;

    if (online) {
      // Cancel any pending offline transition
      if (this._offlineDebounce) {
        clearTimeout(this._offlineDebounce);
        this._offlineDebounce = null;
      }

      if (!this._primaryHostOnline) {
        this._primaryHostOnline = true;
        log.info(`Primary host ${hostId} came ONLINE — starting drain`);
        this.startDrain();
      }
    } else {
      // Debounce OFFLINE — wait to see if ONLINE follows immediately
      if (this._primaryHostOnline && !this._offlineDebounce) {
        this._offlineDebounce = setTimeout(() => {
          this._offlineDebounce = null;
          if (!this._primaryHostOnline) return; // already handled
          this._primaryHostOnline = false;
          log.info(`Primary host ${hostId} went OFFLINE — buffering enabled`);
        }, this._offlineDebounceMs);
      }
    }
  }

  /**
   * Add a record to the buffer. Returns true if buffered, false if published directly.
   */
  add(topic: string, payload: Uint8Array): boolean {
    // If host is online, publish directly — even during drain.
    // Drain handles old buffered data; new data goes straight to the broker.
    if (this._primaryHostOnline) {
      return false;
    }

    const record: BufferedRecord = {
      topic,
      payload,
      bufferedAt: Date.now(),
    };

    // Evict oldest if at capacity
    while (this.records.length >= this.config.maxRecords ||
           this.currentBytes + payload.length > this.config.maxBytes) {
      if (this.records.length === 0) break;
      const evicted = this.records.shift()!;
      this.currentBytes -= evicted.payload.length;
      this._totalEvicted++;
    }

    this.records.push(record);
    this.currentBytes += payload.length;
    this._totalBuffered++;
    return true;
  }

  /**
   * Start draining the buffer (backfill)
   */
  startDrain(): void {
    if (this.records.length === 0) {
      log.info("No records to drain");
      return;
    }

    if (this._draining) {
      log.info("Already draining");
      return;
    }

    this._draining = true;
    this._drainTotal = this.records.length;
    this._drainDrained = 0;
    this._drainStartedAt = Date.now();

    log.info(`Starting drain: ${this._drainTotal} records at ${this.config.drainRate}/s`);

    const batchSize = Math.max(1, Math.ceil(this.config.drainRate / 10));
    this._drainInterval = setInterval(() => {
      if (this.records.length === 0 || !this._primaryHostOnline) {
        this.stopDrain();
        return;
      }

      const batch = this.records.splice(0, batchSize);
      for (const record of batch) {
        this.currentBytes -= record.payload.length;
        this._drainDrained++;
        this._totalDrained++;

        if (this._onPublish) {
          this._onPublish(record.topic, record.payload, true);
        }
      }

      if (this.records.length === 0) {
        this.stopDrain();
      }
    }, 100); // 10 batches per second
  }

  private stopDrain(): void {
    if (this._drainInterval) {
      clearInterval(this._drainInterval);
      this._drainInterval = null;
    }

    if (this._draining) {
      const elapsed = this._drainStartedAt ? (Date.now() - this._drainStartedAt) / 1000 : 0;
      log.info(`Drain complete: ${this._drainDrained} records in ${elapsed.toFixed(1)}s`);
    }

    this._draining = false;
    this._drainStartedAt = null;
  }

  /**
   * Get current state for status queries
   */
  getState(): StoreForwardState {
    const drainRecordsRemaining = this._draining ? this.records.length : 0;
    const drainEta = this._draining && this.config.drainRate > 0
      ? drainRecordsRemaining / this.config.drainRate
      : 0;
    const drainProgress = this._draining && this._drainTotal > 0
      ? (this._drainDrained / this._drainTotal) * 100
      : 0;

    return {
      primaryHostId: this._primaryHostId,
      primaryHostOnline: this._primaryHostOnline,
      bufferedRecords: this.records.length,
      bufferSizeBytes: this.currentBytes,
      bufferCapacityRecords: this.config.maxRecords,
      bufferCapacityBytes: this.config.maxBytes,
      bufferUsedPercentRecords: this.config.maxRecords > 0
        ? (this.records.length / this.config.maxRecords) * 100 : 0,
      bufferUsedPercentBytes: this.config.maxBytes > 0
        ? (this.currentBytes / this.config.maxBytes) * 100 : 0,
      draining: this._draining,
      drainProgress,
      drainRecordsRemaining,
      drainTotalRecords: this._drainTotal,
      drainEtaSeconds: drainEta,
      drainStartedAt: this._drainStartedAt,
      totalBuffered: this._totalBuffered,
      totalDrained: this._totalDrained,
      totalEvicted: this._totalEvicted,
      publishRate: this.getPublishRate(),
    };
  }

  /**
   * Get timeline data for status bar visualization
   */
  getTimeline(): Array<{ timestamp: number; state: "online" | "buffering" | "draining" }> {
    return [...this._timeline];
  }

  /**
   * Cleanup
   */
  destroy(): void {
    this.stopDrain();
    if (this._timelineInterval) {
      clearInterval(this._timelineInterval);
      this._timelineInterval = null;
    }
  }
}
