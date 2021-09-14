package org.astraea.performance;

public class PrintOutThread extends CloseableThread {
  private final Metrics[] producerData;
  private final Metrics[] consumerData;
  private final long records;

  public PrintOutThread(Metrics[] producerData, Metrics[] consumerData, long records) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.records = records;
  }

  @Override
  protected void execute() {
    // 統計producer數據
    long completed = 0;
    long bytes = 0L;
    long max = 0;
    long min = Long.MAX_VALUE;
    for (Metrics data : producerData) {
      completed += data.num();
      bytes += data.bytes();
      if (max < data.max()) max = data.max();
      if (min > data.min()) min = data.min();
    }
    // System.out.println("consumer完成度: "+((double) completed * 100.0 / (double) records)+"%");
    System.out.printf("producers完成度: %.2f%%\n", ((double) completed * 100.0 / (double) records));
    // 印出producers的數據
    System.out.printf("  輸出%.3fMB/second\n", ((double) bytes / (1 << 20)));
    System.out.println("  發送max latency:" + max + "ms");
    System.out.println("  發送mim latency:" + min + "ms");
    for (int i = 0; i < producerData.length; ++i) {
      System.out.printf(
          "  producer[%d]的發送average latency: %.3fms", i, producerData[i].avgLatency());
    }
    // 計算consumer完成度
    completed = 0;
    bytes = 0L;
    max = 0;
    min = Long.MAX_VALUE;
    for (Metrics data : consumerData) {
      completed += data.num();
      bytes += data.bytes();
      if (max < data.max()) max = data.max();
      if (min > data.min()) min = data.min();
    }
    // System.out.println("consumer完成度: "+((double) completed * 100.0 / (double) records)+"%");
    System.out.printf("consumer完成度: %.2f%%\n", ((double) completed * 100.0 / (double) records));
    // 印出consumer的數據
    System.out.printf("  輸入%.3fMB/second\n", ((double) bytes / (1 << 20)));
    System.out.println("  端到端max latency:" + max + "ms");
    System.out.println("  端到端mim latency:" + min + "ms");
    for (int i = 0; i < consumerData.length; ++i) {
      System.out.printf(
          "  consumer[%d]的端到端average latency: %.3fms", i, consumerData[i].avgLatency());
    }
    // 區隔每秒的輸出
    System.out.println("\n");
    // 等待1秒，再抓資料輸出
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {
    }
  }
}
