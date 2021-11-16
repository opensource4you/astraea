package org.astraea.performance;

import java.time.Duration;
import java.util.List;
import org.astraea.concurrent.ThreadPool;

/** Print out the given metrics. Run until `close()` is called. */
public class Tracker implements ThreadPool.Executor {
  private final List<Metrics> producerData;
  private final List<Metrics> consumerData;
  private final long records;
  private final long end;
  private long start = 0L;

  public Tracker(
      List<Metrics> producerData, List<Metrics> consumerData, long records, Duration duration) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.records = records;
    this.end = System.currentTimeMillis() + duration.toMillis();
  }

  @Override
  public State execute() throws InterruptedException {
    if ((logProducers() & logConsumers()) || System.currentTimeMillis() >= end) return State.DONE;
    // 等待1秒，再抓資料輸出
    Thread.sleep(1000);
    return State.RUNNING;
  }

  private boolean logProducers() {
    var completed = 0L;
    var bytes = 0D;
    var max = 0L;
    var min = Long.MAX_VALUE;

    for (Metrics data : producerData) {
      completed += data.num();
      bytes += data.avgBytes();
      max = Math.max(max, data.max());
      min = Math.min(min, data.min());
    }

    if (completed == 0) return false;

    if (start == 0L) start = System.currentTimeMillis();
    var duration = Duration.ofMillis(System.currentTimeMillis() - start);
    System.out.println(
        "Time: "
            + duration.toHoursPart()
            + "hr "
            + duration.toMinutesPart()
            + "min "
            + duration.toSecondsPart()
            + "sec");
    System.out.printf("producers完成度: %.2f%%%n", ((double) completed * 100.0 / (double) records));
    System.out.printf("  輸出%.3fMB/second%n", bytes);
    System.out.println("  發送max latency: " + max + "ms");
    System.out.println("  發送mim latency: " + min + "ms");
    for (int i = 0; i < producerData.size(); ++i) {
      System.out.printf(
          "  producer[%d]的發送average bytes: %.3fMB%n", i, producerData.get(i).avgBytes());
      System.out.printf(
          "  producer[%d]的發送average latency: %.3fms%n", i, producerData.get(i).avgLatency());
    }
    System.out.println("\n");
    return completed >= records;
  }

  private boolean logConsumers() {
    // there is no consumer, so we just complete this log.
    if (consumerData.isEmpty()) return true;
    var completed = 0;
    var bytes = 0D;
    var max = 0L;
    var min = Long.MAX_VALUE;
    for (Metrics data : consumerData) {
      completed += data.num();
      bytes += data.avgBytes();
      max = Math.max(max, data.max());
      min = Math.min(min, data.min());
    }

    if (completed == 0) return false;

    System.out.printf("consumer完成度: %.2f%%%n", ((double) completed * 100.0 / (double) records));
    System.out.printf("  輸入%.3fMB/second%n", bytes);
    System.out.println("  端到端max latency: " + max + "ms");
    System.out.println("  端到端mim latency: " + min + "ms");
    for (int i = 0; i < consumerData.size(); ++i) {
      System.out.printf(
          "  consumer[%d]的端到端average bytes: %.3fMB%n", i, consumerData.get(i).avgBytes());
      System.out.printf(
          "  consumer[%d]的端到端average latency: %.3fms%n", i, consumerData.get(i).avgLatency());
    }
    System.out.println("\n");
    return completed >= records;
  }
}
