package org.astraea.performance;

import java.time.Duration;
import java.util.List;
import org.astraea.concurrent.ThreadPool;

/** Print out the given metrics. Run until `close()` is called. */
public class Tracker implements ThreadPool.Executor {
  private final List<Metrics> producerData;
  private final List<Metrics> consumerData;
  private final DataManager dataManager;
  private final Duration exeTime;
  private long start = System.currentTimeMillis();

  public Tracker(
      List<Metrics> producerData,
      List<Metrics> consumerData,
      DataManager dataManager,
      Duration duration) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.dataManager = dataManager;
    this.exeTime = duration;
  }

  @Override
  public State execute() throws InterruptedException {
    if (logProducers() & logConsumers()) return State.DONE;
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

    if (completed == 0) {
      start = System.currentTimeMillis();
      return false;
    }

    var duration = Duration.ofMillis(System.currentTimeMillis() - start);
    // Print completion rate (by number of records) or (by time)
    var percentage =
        Math.min(
            100D,
            Math.max(
                100D * completed / dataManager.records(),
                100D * (System.currentTimeMillis() - start) / exeTime.toMillis()));

    System.out.println(
        "Time: "
            + duration.toHoursPart()
            + "hr "
            + duration.toMinutesPart()
            + "min "
            + duration.toSecondsPart()
            + "sec");
    System.out.printf("producers完成度: %.2f%%%n", percentage);
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
    return percentage >= 100D;
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

    // Print out percentage of (consumed records) and (produced records)
    var percentage = completed * 100D / dataManager.produced();
    System.out.printf("consumer完成度: %.2f%%%n", percentage);
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
    return completed == dataManager.records()
        || (percentage >= 100D && System.currentTimeMillis() >= start + exeTime.toMillis());
  }
}
