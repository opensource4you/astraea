package org.astraea.performance;

import java.time.Duration;

/** Used to record statistics. This is thread safe. */
public class Metrics {
  private final long startTime = System.currentTimeMillis();
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

  /** @return the average bytes (in second) */
  public synchronized double avgBytes() {
    var duration = Duration.ofMillis(System.currentTimeMillis() - startTime);
    return duration.toSeconds() <= 0
        ? 0
        : ((double) (bytes / duration.toSeconds())) / 1024D / 1024D;
  }

  /** @return total send/received bytes */
  public synchronized long bytes() {
    return bytes;
  }
}
