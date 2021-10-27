package org.astraea.performance;

/** Used to record statistics. This is thread safe. */
public class Metrics {
  private double avgLatency;
  private long num;
  private long max;
  private long min;
  private long bytes;

  public Metrics() {
    avgLatency = 0;
    num = 0;
    max = 0;
    min = Long.MAX_VALUE;
    bytes = 0;
  }

  /** Simultaneously add latency and bytes. */
  public synchronized void put(long latency, long bytes) {
    putLatency(latency);
    addBytes(bytes);
  }
  /** Add a new value to latency metric. */
  public synchronized void putLatency(long latency) {
    if (min > latency) min = latency;
    if (max < latency) max = latency;
    ++num;
    avgLatency += (((double) latency) - avgLatency) / (double) num;
  }
  /** Add a new value to bytes. */
  public synchronized void addBytes(long bytes) {
    this.bytes += bytes;
  }

  /** Get the number of latency put. */
  public synchronized long num() {
    return num;
  }
  /** Get the maximum of latency put. */
  public synchronized long max() {
    return max;
  }
  /** Get the minimum of latency put. */
  public synchronized long min() {
    return min;
  }
  /** Get the average latency. */
  public synchronized double avgLatency() {
    return avgLatency;
  }
  /** Reset to 0 and returns the old value of bytes */
  public synchronized long bytesThenReset() {
    long tmp = this.bytes;
    this.bytes = 0;
    return tmp;
  }
  /** Set all attributes to default value */
  public synchronized void reset() {
    avgLatency = 0;
    num = 0;
    max = 0;
    // 初始為最大的integer值
    min = Long.MAX_VALUE;
    bytes = 0;
  }
}
