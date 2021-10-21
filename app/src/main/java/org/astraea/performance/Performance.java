package org.astraea.performance;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicAdminArgument;
import org.astraea.concurrent.ThreadPool;
import org.astraea.topic.TopicAdmin;

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
    final var param = ArgumentUtil.parseArgument(new Argument(), args);

    try {
      execute(param, ComponentFactory.fromKafka(param.brokers));
    } catch (InterruptedException ignore) {
    }
  }

  public static void execute(final Argument param, ComponentFactory componentFactory)
      throws InterruptedException {
    try (var topicAdmin = TopicAdmin.of(param.adminProps())) {
      topicAdmin.createTopic(param.topic, param.partitions, param.replicas);
    } catch (IOException ignore) {
    }

    final Metrics[] consumerMetric = new Metrics[param.consumers];
    final Metrics[] producerMetric = new Metrics[param.producers];
    for (int i = 0; i < producerMetric.length; ++i) producerMetric[i] = new Metrics();

    // unconditional carry. Let all producers produce the same number of records.
    param.records += param.producers - param.records % param.producers;

    var complete = new CountDownLatch(1);
    try (ThreadPool consumerThreads =
        ThreadPool.builder()
            .executors(
                IntStream.range(0, param.consumers)
                    .mapToObj(
                        i ->
                            consumerExecutor(
                                componentFactory.createConsumer(Collections.singleton(param.topic)),
                                consumerMetric[i] = new Metrics()))
                    .collect(Collectors.toList()))
            .executor(new Tracker(producerMetric, consumerMetric, param.records, complete))
            .build()) {

      System.out.println("Wait for consumer startup");
      Thread.sleep(10000);

      // Close after all records are sent
      try (ThreadPool producerThreads =
          ThreadPool.builder()
              .loop((int) (param.records / param.producers))
              .executors(
                  IntStream.range(0, param.producers)
                      .mapToObj(
                          i ->
                              producerExecutor(
                                  componentFactory.createProducer(), param, producerMetric[i]))
                      .collect(Collectors.toList()))
              .build()) {
        complete.await();
      }
    }
  }

  static ThreadPool.Executor consumerExecutor(Consumer consumer, Metrics metrics) {
    return new ThreadPool.Executor() {
      @Override
      public void execute() throws InterruptedException {
        try {
          for (var record : consumer.poll(Duration.ofSeconds(10))) {
            // 取得端到端延時
            metrics.putLatency(System.currentTimeMillis() - record.timestamp());
            // 記錄輸入byte(沒有算入header和timestamp)
            metrics.addBytes(record.serializedKeySize() + record.serializedValueSize());
          }
        } catch (WakeupException ignore) {
          // Stop polling and being ready to clean up
        }
      }

      @Override
      public void wakeup() {
        consumer.wakeup();
      }

      @Override
      public void cleanup() {
        consumer.cleanup();
      }
    };
  }

  static ThreadPool.Executor producerExecutor(Producer producer, Argument param, Metrics metrics) {
    byte[] payload = new byte[param.recordSize];
    return new ThreadPool.Executor() {
      @Override
      public void execute() throws InterruptedException {
        long start = System.currentTimeMillis();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(param.topic, payload);
        try {
          producer.send(record).get();
          metrics.putLatency(System.currentTimeMillis() - start);
          metrics.addBytes(payload.length);
          Thread.sleep(1);
        } catch (InterruptedException | ExecutionException ignored) {
        }
      }

      @Override
      public void cleanup() {
        producer.cleanup();
      }
    };
  }

  static class Argument extends BasicAdminArgument {

    @Parameter(
        names = {"--topic"},
        description = "String: topic name",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String topic = "testPerformance-" + System.currentTimeMillis();

    @Parameter(
        names = {"--partitions"},
        description = "Integer: number of partitions to create the topic",
        validateWith = ArgumentUtil.PositiveLong.class)
    int partitions = 1;

    @Parameter(
        names = {"--replicas"},
        description = "Integer: number of replica to create the topic",
        validateWith = ArgumentUtil.PositiveLong.class,
        converter = ArgumentUtil.ShortConverter.class)
    short replicas = 1;

    @Parameter(
        names = {"--producers"},
        description = "Integer: number of producers to produce records",
        validateWith = ArgumentUtil.PositiveLong.class)
    int producers = 1;

    @Parameter(
        names = {"--consumers"},
        description = "Integer: number of consumers to consume records",
        validateWith = ArgumentUtil.NonNegativeLong.class)
    int consumers = 1;

    @Parameter(
        names = {"--records"},
        description = "Integer: number of records to send",
        validateWith = ArgumentUtil.NonNegativeLong.class)
    long records = 1000;

    @Parameter(
        names = {"--record.size"},
        description = "Integer: size of each record",
        validateWith = ArgumentUtil.PositiveLong.class)
    int recordSize = 1024;

    @Parameter(
        names = {"--prop.file"},
        description = "String: path to the properties file",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String propFile;

    public Map<String, Object> perfProps() {
      return properties(propFile);
    }
  }
}
