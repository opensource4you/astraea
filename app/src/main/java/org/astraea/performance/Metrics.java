package org.astraea.performance;

import java.util.function.BiConsumer;

/** Used to record statistics. This is thread safe. */
public class Metrics implements BiConsumer<Long, Long> {
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
}
