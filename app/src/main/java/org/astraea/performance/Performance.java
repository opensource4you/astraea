package org.astraea.performance;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionInfo;

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

    execute(param);
  }

  public static void execute(Parameters param) {
    /*=== Initialization ===*/
    final ComponentFactory componentFactory = ComponentFactory.fromKafka(param.brokers);
    checkTopic(componentFactory, param.topic, param.topicConfigs);

    // Start consuming
    final CountDownLatch consumersComplete = new CountDownLatch(1);
    final Metrics[] consumerMetrics = startConsumers(componentFactory, param, consumersComplete);
    // Warm up: Send a record to each partition
    warmUp(componentFactory, param.topic);
    for (var metric : consumerMetrics) metric.reset();
    // Start producing record. (Auto close)
    final Metrics[] producerMetrics = startProducers(componentFactory, param);

    // Get metrics and print them out
    final PrintOutThread printOut =
        new PrintOutThread(producerMetrics, consumerMetrics, param.records);
    printOut.start();

    // Wait for consumer completion
    try {
      consumersComplete.await();
    } catch (InterruptedException ignore) {
    }

    /*=== Clean up ===*/
    printOut.close();
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
    } catch (InterruptedException ignored) {
    } catch (ExecutionException ignored) {
    } catch (Exception ignored) {
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

    // Wait for records all consumed, and cleanup.
    new Thread(
            () -> {
              while (!consumerComplete(consumerMetrics, param.records))
                ;
              consumersComplete.countDown();
              for (var consumer : consumerThreads) consumer.close();
            })
        .start();

    return consumerMetrics;
  }

  public static void warmUp(ComponentFactory componentFactory, String topic) {
    final byte[] payload = new byte[1];

    System.out.println("Warming up...");
    try (Producer producer = componentFactory.createProducer();
        TopicAdmin admin = componentFactory.createAdmin()) {

      List<TopicPartitionInfo> partitions = admin.partitions(topic);
      for (TopicPartitionInfo partition : partitions) {
        // Send record to all partitions
        producer.send(new ProducerRecord<>(topic, partition.partition(), null, payload)).get();
      }
      Thread.sleep(10000);
    } catch (InterruptedException ie) {
    } catch (ExecutionException ee) {
    } catch (Exception e) {
    }
    System.out.println("===================Start testing================\n");
  }

  public static boolean consumerComplete(Metrics[] consumerMetrics, long records) {
    long sum = 0;
    for (var metrics : consumerMetrics) {
      sum += metrics.num();
    }
    return sum >= records;
  }
}
