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
    // 錯誤的格式
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
    // producer要平均分擔發送records，有除不盡的餘數則由第一個producer發送
    producerMetric[0] = new AvgLatency();
    producerThreads[0] =
        new ProducerThread(
            topic,
            bootstrapServers,
            records / numOfProducer + records % numOfProducer,
            recordSize,
            producerMetric[0]);
    for (int i = 1; i < numOfProducer; ++i) {
      producerMetric[i] = new AvgLatency();
      producerThreads[i] =
          new ProducerThread(
              topic, bootstrapServers, records / numOfProducer, recordSize, producerMetric[i]);
    }
    // 啟動consumer
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
    PrintOut printThread = new PrintOut(producerMetric, consumerMetric, records);
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

  // 做文字處理，把參數放到map中。
  // 會把args內以"--"開頭的字作為key，隨後的字作為value，存入Properties中
  // @param args 所有的參數(e.g. {"--topic", "myTopic", "--brokers", "192.168.103.232:9092"})
  private static Properties parseArgs(String[] args) {
    Properties prop = new Properties();
    for (int i = 0; i < args.length; ++i) {
      // 判斷是否是"--"開頭
      if (args[i].charAt(0) == '-' && args[i].charAt(1) == '-') {
        prop.put(args[i].substring(2), args[i + 1]);
        // 跳過一個array element，因為已經作為value被讀取了
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
        // 建立單個topic -> 取得建立的所有topics -> 選擇指定的topic的future -> 等待future完成
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
  private int records;
  private volatile boolean running;

  public PrintOut(AvgLatency[] producerData, AvgLatency[] consumerData, int records) {
    this.producerData = producerData;
    this.consumerData = consumerData;
    this.records = records;
    this.running = true;
  }

  @Override
  public void run() {
    while (running) {
      // 計算producer完成度
      int completed = 0;
      for (int i = 0; i < producerData.length; ++i) {
        completed += producerData[i].getNum();
      }
      System.out.println("producers完成度: " + ((float) completed * 100f / (float) records) + "%");
      // 印出每部producer的數據
      for (int i = 0; i < producerData.length; ++i) {
        System.out.println("producer" + i + ":");
        System.out.println("  輸出" + ((float) producerData[i].getBytes() / 1000000f) + "MB/second");
        System.out.println("  發送average latency:" + producerData[i].getAvg() + "ms");
        System.out.println("  發送max latency:" + producerData[i].getMax() + "ms");
        System.out.println("  發送mim latency:" + producerData[i].getMin() + "ms");
      }
      // 計算consumer完成度
      completed = 0;
      for (int i = 0; i < consumerData.length; ++i) {
        completed += consumerData[i].getNum();
      }
      System.out.println("consumer完成度: " + ((float) completed * 100f / (float) records) + "%");
      // 印出每部consumer的數據
      for (int i = 0; i < consumerData.length; ++i) {
        System.out.println("consumer" + i + ":");
        System.out.println("  輸入" + ((float) consumerData[i].getBytes() / 1000000f) + "MB/second");
        System.out.println("  端到端average latency:" + consumerData[i].getAvg() + "ms");
        System.out.println("  端到端max latency:" + consumerData[i].getMax() + "ms");
        System.out.println("  端到端mim latency:" + consumerData[i].getMin() + "ms");
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
  // 停止繼續印資料
  public void close() {
    running = false;
  }
}
