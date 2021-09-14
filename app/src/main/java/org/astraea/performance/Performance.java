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
      if (brokers.equals("")) throw new IllegalArgumentException("--brokers should be specified");
      if (topic.equals("")) throw new IllegalArgumentException("--topic should be specified");
      if (producers <= 0) throw new IllegalArgumentException("--producers should >= 1");
      if (consumers < 0) throw new IllegalArgumentException("--consumers should be nonegative");
      if (records <= 0) throw new IllegalArgumentException("--records should be nonegative");
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
    checkTopic(componentFactory, param.topic, param.topicConfigs);

    final ProducerThread[] producerThread = new ProducerThread[param.producers];
    final ConsumerThread[] consumerThread = new ConsumerThread[param.consumers];
    final Metrics[] producerMetric = new Metrics[param.producers];
    final Metrics[] consumerMetric = new Metrics[param.consumers];

    startConsumers(consumerThread, componentFactory, consumerMetric, param.topic);
    // Warm up
    System.out.println("Warming up.");
    try (ProducerThread producer =
        new ProducerThread(
            componentFactory.createProducer(), param.topic, 1, 10, new Metrics()); ) {
      producer.start();
      Thread.sleep(5000);
    } catch (Exception ignored) {
    }

    System.out.println("Start producing and consuming.");
    startProducers(producerThread, componentFactory, producerMetric, param);

    // 每秒印出 現在數據
    final PrintOutThread printThread =
        new PrintOutThread(producerMetric, consumerMetric, param.records);
    printThread.start();

    // 等待producers完成
    try {
      for (ProducerThread thread : producerThread) {
        thread.join();
        thread.cleanup();
      }
      Thread.sleep(2000);
    } catch (InterruptedException ignored) {
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
  public static void checkTopic(ComponentFactory componentFactory, String topic, String config) {
    try (TopicAdmin topicAdmin = componentFactory.createAdmin()) {
      // 取得所有topics -> 取得topic名字的future -> 查看topic是否存在
      if (topicAdmin.listTopics().contains(topic)) {
        // Topic already exist
        return;
      }
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
      topicAdmin
          .createTopics(
              Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))
          .get(topic)
          .get();
    } catch (InterruptedException ignored) {
    } catch (ExecutionException ignored) {
    } catch (Exception ignored) {
    }
  }

  // 啟動producers
  private static void startProducers(
      ProducerThread[] producerThreads,
      ComponentFactory componentFactory,
      Metrics[] producerMetrics,
      Parameters param) {
    // producer要平均分擔發送records，有除不盡的餘數則由第一個producer發送
    producerMetrics[0] = new Metrics();
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
      producerMetrics[i] = new Metrics();
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
      Metrics[] consumerMetrics,
      String topic) {
    for (int i = 0; i < consumerMetrics.length; ++i) {
      consumerMetrics[i] = new Metrics();
      consumerThreads[i] =
          new ConsumerThread(
              componentFactory.createConsumer(Collections.singletonList(topic)),
              consumerMetrics[i]);
      consumerThreads[i].start();
    }
  }
}
