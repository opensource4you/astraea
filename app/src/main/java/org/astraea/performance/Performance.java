package org.astraea.performance;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Performance benchmark which includes
 *
 * <ol>
 *   <li>publish latency: the time of completing producer data request
 *   <li>E2E latency: the time for a record to travel through Kafka
 *   <li>input rate: sum of consumer inputs in MByte per second
 *   <li>output rate: sum of producer outputs in MByte per second
 * </ol>
 *
 * With configurations:
 *
 * <ol>
 *   <li>--brokers: the server to connect to
 *   <li>--topic: the topic name
 *   <li>--topicConfig: new topic's configuration Default: partitions:1,replicationFactor:1
 *   <li>--producers: the number of producers (threads). Default: 1
 *   <li>--consumers: the number of consumers (threads). Default: 1
 *   <li>--records: the total number of records sent by the producers.
 *   <li>--recordSize: the record size in byte
 * </ol>
 *
 * To avoid records being produced too fast, producer wait for one millisecond after each send.
 */
public class Performance {
  static class Parameters {
    public final String brokers;
    public final String topic;
    public final String topicConfigs;
    public final int producers;
    public final int consumers;
    public final long records;
    public final int recordSize;

    public Parameters(String brokers, String topic) {
      this(brokers, topic, "", 1, 1, 1000, 1024);
    }

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
          Long.parseLong(prop.getProperty("records", "1000")),
          Integer.parseInt(prop.getProperty("recordSize", "1024")));
    }
  }

  public static void main(String[] args) {
    final Parameters param = Parameters.parseArgs(args);

    try {
      execute(param);
    } catch (InterruptedException ie) {
      System.out.print(ie.getMessage());
    }
  }

  public static void execute(Parameters param) throws InterruptedException {
    /*=== Initialization ===*/
    final ComponentFactory componentFactory = ComponentFactory.fromKafka(param.brokers);
    checkTopic(componentFactory, param.topic, param.topicConfigs);

    // Start consuming
    final CountDownLatch consumersComplete = new CountDownLatch(1);
    final Metrics[] consumerMetrics = startConsumers(componentFactory, param, consumersComplete);

    System.out.println("Wait for consumer startup.");
    Thread.sleep(10000);

    // warm up
    // startProducers(componentFactory, new Parameters(param.brokers, param.topic));
    for (var metric : consumerMetrics) metric.reset();
    System.out.println("============== Start performance benchmark ================");

    // Start producing record. (Auto close)
    final Metrics[] producerMetrics = startProducers(componentFactory, param);

    // Get metrics and print them out.
    PrintOutThread printOutThread =
        new PrintOutThread(producerMetrics, consumerMetrics, param.records);
    printOutThread.start();

    // Keep printing out until consumer consume records more than equal to producers produced.
    printOutThread.join();
    consumersComplete.countDown();
  }

  /** 檢查topic是否存在，不存在就建立新的topic */
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
      System.out.println(
          "Creating topic:\""
              + topic
              + "\" --partitions "
              + partitions
              + " --replicationFactor "
              + replicationFactor);
      // 開始建立topic，並等待topic成功建立
      // 建立單個topic -> 取得建立的所有topics -> 選擇指定的topic的future -> 等待future完成
      topicAdmin
          .createTopics(
              Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))
          .get(topic)
          .get();
    } catch (Exception ignore) {
    }
  }

  // 啟動producers
  public static Metrics[] startProducers(ComponentFactory componentFactory, Parameters param) {
    final Metrics[] producerMetrics = new Metrics[param.producers];
    // producer要平均分擔發送records
    // 啟動producer
    for (int i = 0; i < param.producers; ++i) {
      long records = param.records / param.producers;
      if (i < param.records % param.producers) ++records;
      producerMetrics[i] = new Metrics();
      System.out.println("records: " + records);
      // start up producerThread
      new ProducerThread(
              componentFactory.createProducer(),
              param.topic,
              records,
              param.recordSize,
              producerMetrics[i])
          .start();
    }
    return producerMetrics;
  }

  // Start consumers, and cleanup until countdown latch complete.
  public static Metrics[] startConsumers(
      final ComponentFactory componentFactory,
      final Parameters param,
      final CountDownLatch consumersComplete) {
    final Metrics[] consumerMetrics = new Metrics[param.consumers];
    final ConsumerThread[] consumerThreads = new ConsumerThread[param.consumers];
    for (int i = 0; i < consumerMetrics.length; ++i) {
      consumerMetrics[i] = new Metrics();
      consumerThreads[i] =
          new ConsumerThread(
              componentFactory.createConsumer(Collections.singletonList(param.topic)),
              consumerMetrics[i]);
      consumerThreads[i].start();
    }

    // Cleanup consumers until the consumersComplete is signaled.
    new Thread(
            () -> {
              try {
                consumersComplete.await();
              } catch (InterruptedException ignore) {
              } finally {
                for (var consumer : consumerThreads) consumer.close();
              }
            })
        .start();

    return consumerMetrics;
  }
}
