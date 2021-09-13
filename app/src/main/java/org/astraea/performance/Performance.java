package org.astraea.performance;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;

public class Performance {
  static class Parameters {
    public final String brokers;
    public final String topic;
    public final String topicConfigs;
    public final int producers;
    public final int consumers;
    public final long records;
    public final int recordSize;

    public Parameters(
        String brokers,
        String topic,
        String topicConfigs,
        int producers,
        int consumers,
        long records,
        int recordSize) {
      if (brokers == "") throw new IllegalArgumentException("--brokers should be specified");
      if (topic == "") throw new IllegalArgumentException("--topic should be specified");
      if (producers <= 0) throw new IllegalArgumentException("--producers should >= 1");
      if (consumers < 0) throw new IllegalArgumentException("--consumers should be nonegative");
      if (records < 0) throw new IllegalArgumentException("--records should be nonegative");
      if (recordSize <= 0) throw new IllegalArgumentException("--recordSize should >= 1");

      this.brokers = brokers;
      this.topic = topic;
      this.topicConfigs = topicConfigs;
      this.producers = producers;
      this.consumers = consumers;
      this.records = records;
      this.recordSize = recordSize;
    }
    /*
     * 做文字處理，把參數放到map中。
     * 會把args內以"--"開頭的字作為key，隨後的字作為value，存入Properties中
     * @param args 所有的參數(e.g. {"--topic", "myTopic", "--brokers", "192.168.103.232:9092"})
     */
    public static Parameters parseArgs(String[] args) {
      Properties prop = new Properties();
      for (int i = 0; i < args.length; i += 2) {
        // 判斷是否是"--"開頭
        if (args[i].charAt(0) == '-' && args[i].charAt(1) == '-') {
          prop.put(args[i].substring(2), args[i + 1]);
        } else {
          throw new IllegalArgumentException("parsing error");
        }
      }
      return new Parameters(
          prop.getProperty("brokers", ""),
          prop.getProperty("topic", ""),
          prop.getProperty("topicConfigs", ""),
          Integer.parseInt(prop.getProperty("producers", "1")),
          Integer.parseInt(prop.getProperty("consumers", "1")),
          Long.parseLong(prop.getProperty("records", "1")),
          Integer.parseInt(prop.getProperty("recordSize", "1")));
    }
  }

  public static void main(String[] args) {
    final Parameters param = Parameters.parseArgs(args);

    final ComponentFactory componentFactory = ComponentFactory.fromKafka(param.brokers);
    // 檢查topic是否存在，如果不存在；創建一個
    checkTopic(componentFactory, param);

    final ProducerThread[] producerThread = new ProducerThread[param.producers];
    final ConsumerThread[] consumerThread = new ConsumerThread[param.consumers];
    final AvgLatency[] producerMetric = new AvgLatency[param.producers];
    final AvgLatency[] consumerMetric = new AvgLatency[param.consumers];

    startConsumers(consumerThread, componentFactory, consumerMetric, param.topic);
    // Warm up
    ProducerThread producer =
        new ProducerThread(componentFactory.createProducer(), param.topic, 1, 10, new AvgLatency());
    try {
      Thread.sleep(5000);
    } catch (Exception e) {
    }
    startProducers(producerThread, componentFactory, producerMetric, param);

    // 每秒印出 現在數據
    final PrintOut printThread = new PrintOut(producerMetric, consumerMetric, param.records);
    printThread.start();

    // 等待producers完成
    try {
      for (int i = 0; i < producerThread.length; ++i) {
        producerThread[i].join();
        producerThread[i].cleanup();
      }
      Thread.sleep(2000);
    } catch (InterruptedException ie) {
    }
    // 關閉consumer
    for (var i : consumerThread) {
      i.close();
    }
    // 把印數據的thread關掉
    printThread.close();

    System.out.println("Performance end.");
  }
  // 檢查topic是否存在，不存在就建立新的topic
  private static void checkTopic(ComponentFactory componentFactory, Parameters param) {
    try (TopicAdmin topicAdmin = componentFactory.createAdmin()) {
      // 取得所有topics -> 取得topic名字的future -> 查看topic是否存在
      if (topicAdmin.listTopics().contains(param.topic)) {
        // Topic already exist
        return;
      }
      // 新建立topic
      System.out.println("Create a new topic");
      // 使用給定的設定來建立topic
      int partitions = 1;
      short replicationFactor = 1;
      // 判斷是否有topicConfigs
      String config = param.topicConfigs;
      if (config == null) {
        System.out.println("No given topicConfigs. Use default.");
      } else {
        // 切割config字串
        String[] configs = config.split("[:,]");
        for (int i = 0; i < configs.length; ++i) {
          // 找出key的值是什麼，並賦予對應的值
          if (configs[i].equals("partitions")) {
            partitions = Integer.parseInt(configs[i + 1]);
            ++i;
          } else if (configs[i].equals("replicationFactor") || configs[i].equals("replicas")) {
            replicationFactor = Short.parseShort(configs[i + 1]);
            ++i;
          }
        }
      }
      // 開始建立topic，並等待topic成功建立
      System.out.println(
          "Creating topic:\""
              + param.topic
              + "\" --partitions "
              + partitions
              + " --replicationFactor "
              + replicationFactor);
      // 建立單個topic -> 取得建立的所有topics -> 選擇指定的topic的future -> 等待future完成
      topicAdmin
          .createTopics(
              Collections.singletonList(new NewTopic(param.topic, partitions, replicationFactor)))
          .values()
          .get(param.topic)
          .get();
    } catch (InterruptedException ie) {
    } catch (ExecutionException ee) {
    } catch (Exception e) {
    }
  }

  // 啟動producers
  private static void startProducers(
      ProducerThread[] producerThreads,
      ComponentFactory componentFactory,
      AvgLatency[] producerMetrics,
      Parameters param) {
    // producer要平均分擔發送records，有除不盡的餘數則由第一個producer發送
    producerMetrics[0] = new AvgLatency();
    producerThreads[0] =
        new ProducerThread(
            componentFactory.createProducer(),
            param.topic,
            param.records / param.producers + param.records % param.producers,
            param.recordSize,
            producerMetrics[0]);
    producerThreads[0].start();
    // 啟動producer
    for (int i = 1; i < param.producers; ++i) {
      producerMetrics[i] = new AvgLatency();
      producerThreads[i] =
          new ProducerThread(
              componentFactory.createProducer(),
              param.topic,
              param.records / param.producers,
              param.recordSize,
              producerMetrics[i]);
      producerThreads[i].start();
    }
  }
  // 啟動consumers
  private static void startConsumers(
      ConsumerThread[] consumerThreads,
      ComponentFactory componentFactory,
      AvgLatency[] consumerMetrics,
      String topic) {
    for (int i = 0; i < consumerMetrics.length; ++i) {
      consumerMetrics[i] = new AvgLatency();
      consumerThreads[i] =
          new ConsumerThread(
              componentFactory.createConsumer(Collections.singletonList(topic)),
              consumerMetrics[i]);
      consumerThreads[i].start();
    }
  }
  // 印數據
  static class PrintOut extends CloseableThread {
    private final AvgLatency[] producerData;
    private final AvgLatency[] consumerData;
    private final long records;
    private volatile boolean running;

    public PrintOut(AvgLatency[] producerData, AvgLatency[] consumerData, long records) {
      this.producerData = producerData;
      this.consumerData = consumerData;
      this.records = records;
      this.running = true;
    }

    @Override
    public void execute() {
      // 統計producer數據
      long completed = 0;
      long bytes = 0l;
      long max = 0;
      long min = Long.MAX_VALUE;
      for (int i = 0; i < producerData.length; ++i) {
        completed += producerData[i].num();
        bytes += producerData[i].bytes();
        if (max < producerData[i].max()) max = producerData[i].max();
        if (min > producerData[i].min()) min = producerData[i].min();
      }
      System.out.printf("producers完成度: %2f%\n", ((double) completed * 100.0 / (double) records));
      // 印出producers的數據
      System.out.printf("  輸出MB/second", ((double) bytes / (1 << 20)));
      System.out.println("  發送max latency:" + max + "ms");
      System.out.println("  發送mim latency:" + min + "ms");
      for (int i = 0; i < producerData.length; ++i) {
        System.out.println(
            "  producer[" + i + "]的發送average latency:" + producerData[i].avg() + "ms");
      }
      // 計算consumer完成度
      completed = 0;
      bytes = 0l;
      max = 0;
      min = Long.MAX_VALUE;
      for (int i = 0; i < consumerData.length; ++i) {
        completed += consumerData[i].num();
        bytes += consumerData[i].bytes();
        if (max < consumerData[i].max()) max = consumerData[i].max();
        if (min > consumerData[i].min()) min = consumerData[i].min();
      }
      System.out.printf("consumer完成度: %2f%%");
      // 印出consumer的數據
      System.out.printf("  輸入%4fMB/second\n", ((double) bytes / (1 << 20)));
      System.out.println("  端到端max latency:" + max + "ms");
      System.out.println("  端到端mim latency:" + min + "ms");
      for (int i = 0; i < consumerData.length; ++i) {
        System.out.println(
            "  consumer[" + i + "]的端到端average latency:" + consumerData[i].avg() + "ms");
      }
      // 區隔每秒的輸出
      System.out.println("\n");
      // 等待1秒，再抓資料輸出
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
    }
  }
}
