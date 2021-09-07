package org.astraea.performance;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

public class Performance {
  public static void main(String[] args) {
    // 把參數放到map中，方便之後取用
    Properties prop = parseArgs(args);
    if (prop == null) return;

    // 取得參數
    String topic = prop.getProperty("topic");
    String bootstrapServers = prop.getProperty("bootstrapServers");
    if (bootstrapServers == null) {
      bootstrapServers = prop.getProperty("brokers");
    }
    int numOfProducer = 1;
    if (prop.getProperty("producers") != null) {
      numOfProducer = Integer.parseInt(prop.getProperty("producers"));
    }
    int numOfConsumer = 1;
    if (prop.getProperty("consumers") != null) {
      numOfConsumer = Integer.parseInt(prop.getProperty("consumers"));
    }
    int records = Integer.parseInt(prop.getProperty("records"));
    int recordSize = Integer.parseInt(prop.getProperty("recordSize"));

    // 檢查topic是否存在
    checkTopic(bootstrapServers, topic, prop.getProperty("topicConfigs"));

    // 全域使用的統計物件
    AvgLatency[] producerMetric = new AvgLatency[numOfProducer];
    AvgLatency[] consumerMetric = new AvgLatency[numOfConsumer];

    // 新增多個thread，用來執行produce/consume
    ProducerThread[] producerThreads = new ProducerThread[numOfProducer];
    ConsumerThread[] consumerThreads = new ConsumerThread[numOfConsumer];
    for (int i = 0; i < numOfProducer; ++i) {
      producerMetric[i] = new AvgLatency();
      producerThreads[i] =
          new ProducerThread(topic, bootstrapServers, records, recordSize, producerMetric[i]);
    }
    for (int i = 0; i < numOfConsumer; ++i) {
      consumerMetric[i] = new AvgLatency();
      consumerThreads[i] =
          new ConsumerThread(topic, bootstrapServers, "groupId", consumerMetric[i]);
    }

    // 執行所有consumer/producer threads
    System.out.println("Consumers starting...");
    for (int i = 0; i < numOfConsumer; ++i) {
      consumerThreads[i].start();
    }
    System.out.println("Producer starting...");
    for (int i = 0; i < numOfProducer; ++i) {
      producerThreads[i].start();
    }

    // 每秒印出 現在數據
    PrintOut printThread = new PrintOut(producerMetric, consumerMetric);
    printThread.start();

    // 等待producers完成
    for (ProducerThread thread : producerThreads) {
      try {
        thread.join();
      } catch (InterruptedException ie) {
      }
    }
    // 把consumer關閉
    for (ConsumerThread thread : consumerThreads) {
      thread.close();
    }
    // 把印數據的thread關掉
    printThread.close();

    System.out.println("Performance end.");
  }

  // 做文字處理，把參數放到map中
  private static Properties parseArgs(String[] args) {
    Properties prop = new Properties();
    for (int i = 0; i < args.length; ++i) {
      if (args[i].charAt(0) == '-' && args[i].charAt(1) == '-') {
        prop.put(args[i].substring(2), args[i + 1]);
        ++i;
      } else {
        System.out.println("Parsing error.");
        return null;
      }
    }
    return prop;
  }
  // 檢查topic是否存在，不存在就建立新的topic
  private static void checkTopic(String bootstrapServers, String topic, String config) {
    Properties prop = new Properties();
    prop.put("bootstrap.servers", bootstrapServers);
    try (Admin admin = Admin.create(prop)) {
      // 取得所有topics -> 取得topic名字的future -> 查看topic是否存在
      if (admin.listTopics().names().get().contains(topic)) {
        System.out.println("Topic already exists");
        return;
      }
      System.out.println("Topic:\"" + topic + "\" not found.");
      // 新建立topic
      System.out.println("Create a new topic");
      // 使用給定的設定來建立topic
      int partitions = 1;
      short replicationFactor = 1;
      // 判斷是否有topicConfigs
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
        // 開始建立topic，並等待topic成功建立
        System.out.println(
            "Creating topic:\""
                + topic
                + "\" --partitions "
                + partitions
                + " --replicationFactor "
                + replicationFactor);
        admin
            .createTopics(Collections.singleton(new NewTopic(topic, partitions, replicationFactor)))
            .values()
            .get(topic)
            .get();
      }
    } catch (InterruptedException ie) {
    } catch (ExecutionException ee) {
    }
  }
}
// 印數據
class PrintOut extends Thread {
  private AvgLatency[] producerData;
  private AvgLatency[] consumerData;
  private volatile boolean running;

  public PrintOut(AvgLatency[] producerData, AvgLatency[] consumerData) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.running = true;
  }

  @Override
  public void run() {
    while (running) {
      for (int i = 0; i < producerData.length; ++i) {
        System.out.println("producer" + i + ":");
        System.out.println("  輸出" + ((float) producerData[i].getBytes() / 1000000f) + "MB/second");
        System.out.println("  發送average latency:" + producerData[i].getAvg() + "ms");
        System.out.println("  發送max latency:" + producerData[i].getMax() + "ms");
        System.out.println("  發送mim latency:" + producerData[i].getMin() + "ms");
      }
      for (int i = 0; i < consumerData.length; ++i) {
        System.out.println("consumer" + i + ":");
        System.out.println("  輸入" + ((float) consumerData[i].getBytes() / 1000000f) + "MB/second");
        System.out.println("  端到端average latency:" + consumerData[i].getAvg() + "ms");
        System.out.println("  端到端max latency:" + consumerData[i].getMax() + "ms");
        System.out.println("  端到端mim latency:" + consumerData[i].getMin() + "ms");
      }
      System.out.println("\n");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
    }
  }
  // 停止繼續印資料
  public void close() {
    running = false;
  }
}
