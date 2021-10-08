package org.astraea.performance;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.admin.NewTopic;
import org.astraea.argument.ArgumentUtil;

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
 *   <li>--topic: the topic name. Create new topic when the given topic does not exist. Default:
 *       "testPerformance-" + System.currentTimeMillis()
 *   <li>--partitions: topic config when creating new topic. Default: 1
 *   <li>--replicationFactor: topic config when creating new topic. Default: 1
 *   <li>--producers: the number of producers (threads). Default: 1
 *   <li>--consumers: the number of consumers (threads). Default: 1
 *   <li>--records: the total number of records sent by the producers. Default: 1000
 *   <li>--recordSize: the record size in byte. Default: 1024
 * </ol>
 *
 * To avoid records being produced too fast, producer wait for one millisecond after each send.
 */
public class Performance {

  public static void main(String[] args) {
    final var param = ArgumentUtil.parseArgument(new PerformanceArgument(), args);

    try {
      execute(param);
    } catch (InterruptedException ignore) {
    }
  }

  public static void execute(final PerformanceArgument param) throws InterruptedException {
    /*=== Initialization ===*/
    final ComponentFactory componentFactory = ComponentFactory.fromKafka(param.brokers);
    checkTopic(componentFactory, param);

    // Start consuming
    final CountDownLatch consumersComplete = new CountDownLatch(1);
    final Metrics[] consumerMetrics = startConsumers(componentFactory, param, consumersComplete);

    System.out.println("Wait for consumer startup.");
    Thread.sleep(10000);
    System.out.println("============== Start performance benchmark ================");

    // Start producing record. (Auto close)
    final Metrics[] producerMetrics = startProducers(componentFactory, param);

    try (PrintOutThread printOutThread =
        new PrintOutThread(producerMetrics, consumerMetrics, param.records)) {
      printOutThread.start();
      // Check consumed records every one second. (Process blocks here.)
      checkConsume(consumerMetrics, param.records);
    } catch (InterruptedException ignore) {
    } finally {
      // Stop all consumers
      consumersComplete.countDown();
    }
  }

  /** 檢查topic是否存在，不存在就建立新的topic */
  public static void checkTopic(ComponentFactory componentFactory, PerformanceArgument param) {
    try (TopicAdmin topicAdmin = componentFactory.createAdmin()) {
      // 取得所有topics -> 取得topic名字的future -> 查看topic是否存在
      if (topicAdmin.listTopics().contains(param.topic)) {
        // Topic already exist
        return;
      }
      // 新建立topic
      System.out.println(
          "Creating topic:\""
              + param.topic
              + "\" --partitions "
              + param.partitions
              + " --replicationFactor "
              + param.replicationFactor);
      // 開始建立topic，並等待topic成功建立
      // 建立單個topic -> 取得建立的所有topics -> 選擇指定的topic的future -> 等待future完成
      topicAdmin
          .createTopics(
              Collections.singletonList(
                  new NewTopic(param.topic, param.partitions, param.replicationFactor)))
          .get(param.topic)
          .get();
    } catch (Exception ignore) {
    }
  }

  // 啟動producers
  public static Metrics[] startProducers(
      ComponentFactory componentFactory, PerformanceArgument param) {
    final Metrics[] producerMetrics = new Metrics[param.producers];
    // producer平均分擔發送records
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
      final PerformanceArgument param,
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

  // Block until records consumed reached `records`
  private static void checkConsume(final Metrics[] consumerMetrics, final long records)
      throws InterruptedException {
    int sum = 0;
    while (sum < records) {
      sum = 0;
      for (Metrics metric : consumerMetrics) sum += metric.num();
      Thread.sleep(1000);
    }
  }
}
