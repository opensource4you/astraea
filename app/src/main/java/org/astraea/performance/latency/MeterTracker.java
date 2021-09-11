package org.astraea.performance.latency;

import java.util.concurrent.TimeUnit;

class MeterTracker extends CloseableThread {
  private final long startTime = System.currentTimeMillis();
  private final String name;
  private long bytes = 0;
  private long count = 0;
  private double avgLatency = 0;
  private long maxLatency = 0;

  MeterTracker(String name) {
    this.name = name;
  }

  synchronized void record(long bytes, long latency) {
    maxLatency = Math.max(maxLatency, latency);
    avgLatency = (avgLatency * count + latency) / (count + 1);
    count += 1;
    this.bytes += bytes;
  }

  synchronized long count() {
    return count;
  }

  synchronized long bytes() {
    return bytes;
  }

  synchronized double averageLatency() {
    return avgLatency;
  }

  synchronized long maxLatency() {
    return maxLatency;
  }

  synchronized double throughput() {
    var second = (System.currentTimeMillis() - startTime) / 1000;
    return ((double) bytes / second);
  }

  private static double round(double value) {
    return Math.round(value * 100) / (double) 100;
  }

  @Override
  public synchronized String toString() {
    return " records: "
        + count
        + " throughput: "
        + round(throughput() / 1024 / 1024)
        + " MB/s"
        + " avg_latency: "
        + round(avgLatency)
        + " ms"
        + " max_latency: "
        + maxLatency
        + " ms";
  }

  @Override
  void execute() {
    try {
      TimeUnit.SECONDS.sleep(2);
      String output;
      synchronized (this) {
        if (count == 0) return;
        else output = "[" + name + "] " + this;
      }
      System.out.println(output);
    } catch (InterruptedException e) {
      // swallow
    }
  }
}
