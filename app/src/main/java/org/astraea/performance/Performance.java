package org.astraea.performance;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.producer.Producer;
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

  public static void main(String[] args)
      throws InterruptedException, IOException, ExecutionException {
    execute(ArgumentUtil.parseArgument(new Argument(), args));
  }

  public static void execute(final Argument param)
      throws InterruptedException, IOException, ExecutionException {
    try (var topicAdmin = TopicAdmin.of(param.perfProps())) {
      topicAdmin
          .creator()
          .numberOfReplicas(param.replicas)
          .numberOfPartitions(param.partitions)
          .topic(param.topic)
          .create();
    }

    var consumerMetrics =
        IntStream.range(0, param.consumers)
            .mapToObj(i -> new Metrics())
            .collect(Collectors.toUnmodifiableList());
    var producerMetrics =
        IntStream.range(0, param.producers)
            .mapToObj(i -> new Metrics())
            .collect(Collectors.toUnmodifiableList());

    var consumerRecords = new AtomicLong(param.records);
    var producerRecords = new AtomicLong(param.records);
    var tracker = new Tracker(producerMetrics, consumerMetrics, param.records);
    var groupId = "groupId-" + System.currentTimeMillis();
    var countDown = new CountDownLatch(param.consumers);
    try (ThreadPool threadPool =
        ThreadPool.builder()
            .executors(
                IntStream.range(0, param.consumers)
                    .mapToObj(
                        i ->
                            consumerExecutor(
                                Consumer.builder()
                                    .brokers(param.brokers)
                                    .topics(Set.of(param.topic))
                                    .groupId(groupId)
                                    .configs(param.perfProps())
                                    .assignedListener(ignore -> countDown.countDown())
                                    .build(),
                                consumerMetrics.get(i),
                                consumerRecords))
                    .collect(Collectors.toUnmodifiableList()))
            .executors(
                IntStream.range(0, param.producers)
                    .mapToObj(
                        i ->
                            producerExecutor(
                                Producer.builder()
                                    .configs(param.perfProps())
                                    .partitionClassName(param.partitioner)
                                    .build(),
                                param,
                                producerMetrics.get(i),
                                producerRecords,
                                countDown))
                    .collect(Collectors.toUnmodifiableList()))
            .executor(tracker)
            .build()) {
      threadPool.waitAll();
    }
  }

  static ThreadPool.Executor consumerExecutor(
      Consumer<byte[], byte[]> consumer, Metrics metrics, AtomicLong records) {
    return new ThreadPool.Executor() {
      @Override
      public State execute() {
        try {
          consumer
              .poll(Duration.ofSeconds(10))
              .forEach(
                  record -> {
                    // 記錄端到端延時, 記錄輸入byte(沒有算入header和timestamp)
                    metrics.put(
                        System.currentTimeMillis() - record.timestamp(),
                        record.serializedKeySize() + record.serializedValueSize());
                    records.decrementAndGet();
                  });
          return records.get() <= 0 ? State.DONE : State.RUNNING;
        } catch (WakeupException ignore) {
          // Stop polling and being ready to clean up
          return State.DONE;
        }
      }

      @Override
      public void wakeup() {
        consumer.wakeup();
      }

      @Override
      public void close() {
        consumer.close();
      }
    };
  }

  static ThreadPool.Executor producerExecutor(
      Producer<byte[], byte[]> producer,
      Argument param,
      Metrics metrics,
      AtomicLong records,
      CountDownLatch countDown) {
    byte[] payload = new byte[param.recordSize];
    return new ThreadPool.Executor() {
      @Override
      public State execute() {
        try {
          countDown.await();
        } catch (InterruptedException ignore) {
        }
        var currentRecords = records.getAndDecrement();
        if (currentRecords <= 0) return State.DONE;
        long start = System.currentTimeMillis();
        producer
            .sender()
            .topic(param.topic)
            .value(payload)
            .timestamp(start)
            .run()
            .whenComplete(
                (m, e) -> metrics.put(System.currentTimeMillis() - start, payload.length));
        return State.RUNNING;
      }

      @Override
      public void close() {
        producer.close();
      }
    };
  }

  static class Argument extends BasicArgument {

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
        names = {"--jmx.servers"},
        description =
            "String: server to get jmx metrics <jmx_server>@<broker_id>[,<jmx_server>@<broker_id>]*")
    String jmxServers = "";

    @Parameter(
        names = {"--partitioner"},
        description = "String: the full class name of the desired partitioner",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String partitioner = DefaultPartitioner.class.getName();

    @Parameter(
        names = {"--prop.file"},
        description = "String: path to the properties file",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String propFile;

    public Map<String, Object> perfProps() {
      Map<String, Object> props = properties(propFile);
      props.put("jmx_servers", this.jmxServers);
      return props;
    }
  }
}
