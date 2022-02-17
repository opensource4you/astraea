package org.astraea.performance;

import java.util.function.BiConsumer;
import org.apache.kafka.common.Metric;

/** Used to record statistics. This is thread safe. */
public class Metrics implements BiConsumer<Long, Long> {
  private double avgLatency;
  private long num;
  private long max;
  private long min;
  private long bytes;
  private long currentBytes;
  private Metric realBytes = null;
  private long lastRealBytes = 0L;

  public Metrics() {
    avgLatency = 0;
    num = 0;
    max = 0;
    min = Long.MAX_VALUE;
    bytes = 0;
    currentBytes = 0;
  }

  /** Simultaneously add latency and bytes. */
  @Override
  public synchronized void accept(Long latency, Long bytes) {
    ++num;
    putLatency(latency);
    addBytes(bytes);
  }
  /** Add a new value to latency metric. */
  private synchronized void putLatency(long latency) {
    min = Math.min(min, latency);
    max = Math.max(max, latency);
    avgLatency += (((double) latency) - avgLatency) / (double) num;
  }
  /** Add a new value to bytes. */
  private synchronized void addBytes(long bytes) {
    this.currentBytes += bytes;
    this.bytes += bytes;
  }

  /** @return Get the number of latency put. */
  public synchronized long num() {
    return num;
  }
  /** @return Get the maximum of latency put. */
  public synchronized long max() {
    return max;
  }
  /** @return Get the minimum of latency put. */
  public synchronized long min() {
    return min;
  }
  /** @return Get the average latency. */
  public synchronized double avgLatency() {
    return avgLatency;
  }

  /** @return total send/received bytes */
  public synchronized long bytes() {
    return bytes;
  }

  public synchronized long clearAndGetCurrentBytes() {
    var ans = currentBytes;
    currentBytes = 0;
    return ans;
  }

  public void setRealBytesMetric(Metric realBytesMetric) {
    this.realBytes = realBytesMetric;
  }

  /**
   * Get current real bytes since last call. Warning: Real bytes is recorded in `realBytes`. Please
   * #setRealBytesMetric before get #currentRealBytes
   */
  public synchronized long currentRealBytes() {
    if (realBytes != null && realBytes.metricValue() instanceof Double) {
      var totalRealBytes = (long) Double.parseDouble(realBytes.metricValue().toString());
      var ans = totalRealBytes - lastRealBytes;
      lastRealBytes = totalRealBytes;
      return ans;
    } else {
      return 0L;
    }
  }
}
