package org.astraea.performance;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.astraea.concurrent.ThreadPool;

/** Print out the given metrics. Run until `close()` is called. */
public class Tracker implements ThreadPool.Executor {
  private final List<Metrics> producerData;
  private final List<Metrics> consumerData;
  private final long records;
  private long start = 0L;

  public Tracker(List<Metrics> producerData, List<Metrics> consumerData, long records) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.records = records;
  }

  @Override
  public State execute() throws InterruptedException {
    if (logProducers() & logConsumers()) return State.DONE;
    // 等待1秒，再抓資料輸出
    Thread.sleep(1000);
    return State.RUNNING;
  }

  private Duration duration() {
    if (start == 0L) start = System.currentTimeMillis();
    return Duration.ofMillis(System.currentTimeMillis() - start);
  }

  private static double avg(Duration duration, long value) {
    return duration.toSeconds() <= 0
        ? 0
        : ((double) (value / duration.toSeconds())) / 1024D / 1024D;
  }

  private boolean logProducers() {
    var result = result(producerData);
    if (result.completedRecords == 0) return false;

    var duration = duration();
    System.out.println(
        "Time: "
            + duration.toHoursPart()
            + "hr "
            + duration.toMinutesPart()
            + "min "
            + duration.toSecondsPart()
            + "sec");
    System.out.printf(
        "producers完成度: %.2f%%%n", ((double) result.completedRecords * 100.0 / (double) records));
    System.out.printf("  輸出%.3fMB/second%n", result.averageBytes(duration));
    System.out.println("  發送max latency: " + result.maxLatency + "ms");
    System.out.println("  發送mim latency: " + result.minLatency + "ms");
    for (int i = 0; i < result.bytes.size(); ++i) {
      System.out.printf(
          "  producer[%d]的發送average bytes: %.3fMB%n", i, avg(duration, result.bytes.get(i)));
      System.out.printf(
          "  producer[%d]的發送average latency: %.3fms%n", i, result.averageLatencies.get(i));
    }
    System.out.println("\n");
    return result.completedRecords >= records;
  }

  private boolean logConsumers() {
    // there is no consumer, so we just complete this log.
    if (consumerData.isEmpty()) return true;
    var result = result(consumerData);
    if (result.completedRecords == 0) return false;
    var duration = duration();

    System.out.printf(
        "consumer完成度: %.2f%%%n", ((double) result.completedRecords * 100.0 / (double) records));
    System.out.printf("  輸入%.3fMB/second%n", result.averageBytes(duration));
    System.out.println("  端到端max latency: " + result.maxLatency + "ms");
    System.out.println("  端到端mim latency: " + result.minLatency + "ms");
    for (int i = 0; i < result.bytes.size(); ++i) {
      System.out.printf(
          "  consumer[%d]的端到端average bytes: %.3fMB%n", i, avg(duration, result.bytes.get(i)));
      System.out.printf(
          "  consumer[%d]的端到端average latency: %.3fms%n", i, result.averageLatencies.get(i));
    }
    System.out.println("\n");
    return result.completedRecords >= records;
  }

  private static Result result(List<Metrics> metrics) {
    var completed = 0;
    var bytes = new ArrayList<Long>();
    var averageLatencies = new ArrayList<Double>();
    var max = 0L;
    var min = Long.MAX_VALUE;
    for (Metrics data : metrics) {
      completed += data.num();
      bytes.add(data.bytes());
      averageLatencies.add(data.avgLatency());
      max = Math.max(max, data.max());
      min = Math.min(min, data.min());
    }
    return new Result(
        completed,
        Collections.unmodifiableList(bytes),
        Collections.unmodifiableList(averageLatencies),
        min,
        max);
  }

  private static class Result {
    public final long completedRecords;
    public final List<Long> bytes;
    public final List<Double> averageLatencies;
    public final long minLatency;
    public final long maxLatency;

    Result(
        long completedRecords,
        List<Long> bytes,
        List<Double> averageLatencies,
        long minLatency,
        long maxLatency) {
      this.completedRecords = completedRecords;
      this.bytes = bytes;
      this.averageLatencies = averageLatencies;
      this.minLatency = minLatency;
      this.maxLatency = maxLatency;
    }

    double averageBytes(Duration duration) {
      return avg(duration, totalBytes());
    }

    long totalBytes() {
      return bytes.stream().mapToLong(i -> i).sum();
    }
  }
}
