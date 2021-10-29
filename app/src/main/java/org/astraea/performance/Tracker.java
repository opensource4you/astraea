package org.astraea.performance;

import java.util.List;
import org.astraea.concurrent.ThreadPool;

/** Print out the given metrics. Run until `close()` is called. */
public class Tracker implements ThreadPool.Executor {
  private final List<Metrics> producerData;
  private final List<Metrics> consumerData;
  private final long records;

  public Tracker(List<Metrics> producerData, List<Metrics> consumerData, long records) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.records = records;
  }

  @Override
  public State execute() throws InterruptedException {
    long completed = 0;
    long bytes = 0L;
    long max = 0;
    long min = Long.MAX_VALUE;

    /* producer */
    for (Metrics data : producerData) {
      completed += data.num();
      bytes += data.bytesThenReset();
      if (max < data.max()) max = data.max();
      if (min > data.min()) min = data.min();
    }
    if (completed == 0) {
      Thread.sleep(1000);
      return State.RUNNING;
    }
    System.out.printf("producers完成度: %.2f%%%n", ((double) completed * 100.0 / (double) records));
    System.out.printf("  輸出%.3fMB/second%n", ((double) bytes / (1 << 20)));
    System.out.println("  發送max latency: " + max + "ms");
    System.out.println("  發送mim latency: " + min + "ms");
    for (int i = 0; i < producerData.size(); ++i) {
      System.out.printf(
          "  producer[%d]的發送average latency: %.3fms%n", i, producerData.get(i).avgLatency());
    }
    /* consumer */
    completed = 0;
    bytes = 0L;
    max = 0;
    min = Long.MAX_VALUE;
    for (Metrics data : consumerData) {
      completed += data.num();
      bytes += data.bytesThenReset();
      if (max < data.max()) max = data.max();
      if (min > data.min()) min = data.min();
    }
    System.out.printf("consumer完成度: %.2f%%%n", ((double) completed * 100.0 / (double) records));
    System.out.printf("  輸入%.3fMB/second%n", ((double) bytes / (1 << 20)));
    System.out.println("  端到端max latency: " + max + "ms");
    System.out.println("  端到端mim latency: " + min + "ms");
    for (int i = 0; i < consumerData.size(); ++i) {
      System.out.printf(
          "  consumer[%d]的端到端average latency: %.3fms%n", i, consumerData.get(i).avgLatency());
    }

    System.out.println("\n");
    // The consumer has consumed the target records
    if (completed >= records) return State.DONE;
    // 等待1秒，再抓資料輸出
    Thread.sleep(1000);
    return State.RUNNING;
  }
}
