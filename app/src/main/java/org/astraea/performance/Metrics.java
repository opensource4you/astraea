package org.astraea.performance;

import java.util.concurrent.atomic.LongAdder;

/** Used to track statistics. */
public class Metrics {
  private double avgLatency;
  private long num;
  private long max;
  private long min;
  private final LongAdder bytes;

  public Metrics() {
    avgLatency = 0;
    num = 0;
    max = 0;
    min = Long.MAX_VALUE;
    bytes = new LongAdder();
  }
  /** Add a new value to latency metric. */
  public void putLatency(long latency) {
    if (min > latency) min = latency;
    if (max < latency) max = latency;
    ++num;
    avgLatency += (((double) latency) - avgLatency) / (double) num;
  }
  /** Add a new value to bytes. */
  public void addBytes(long bytes) {
    this.bytes.add(bytes);
  }

  /** Get the number of latency put. */
  public long num() {
    return num;
  }
  /** Get the maximum of latency put. */
  public long max() {
    return max;
  }
  /** Get the minimum of latency put. */
  public long min() {
    return min;
  }
  /** Get the average latency. */
  public double avgLatency() {
    return avgLatency;
  }
  /** Reset to 0 and returns the old value of bytes */
  public long bytesThenReset() {
    return this.bytes.sumThenReset();
  }
  /** Set all attributes to default value */
  public void reset() {
    avgLatency = 0;
    num = 0;
    max = 0;
    // 初始為最大的integer值
    min = Long.MAX_VALUE;
    bytes.reset();
  }
}
