package org.astraea.performance;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.CompressionType;
import org.astraea.Utils;
import org.astraea.argument.CompressionField;
import org.astraea.argument.NonEmptyStringField;
import org.astraea.argument.NonNegativeShortField;
import org.astraea.argument.PositiveLongField;
import org.astraea.argument.PositiveShortField;
import org.astraea.argument.StringMapField;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.State;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.producer.Producer;
import org.astraea.producer.TransactionalProducer;
import org.astraea.topic.TopicAdmin;
import org.astraea.utils.DataSize;
import org.astraea.utils.DataUnit;

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
  /** Used in Automation, to achieve the end of one Performance and then start another. */
  public static void main(String[] args)
      throws InterruptedException, IOException, ExecutionException {
    execute(org.astraea.argument.Argument.parse(new Argument(), args));
  }

  public static Result execute(final Argument param)
      throws InterruptedException, IOException, ExecutionException {
    List<Integer> partitions;
    try (var topicAdmin = TopicAdmin.of(param.props())) {
      topicAdmin
          .creator()
          .numberOfReplicas(param.replicas)
          .numberOfPartitions(param.partitions)
          .topic(param.topic)
          .create();

      Utils.waitFor(() -> topicAdmin.topicNames().contains(param.topic));
      partitions = partition(param, topicAdmin);
    }

    var consumerMetrics =
        IntStream.range(0, param.consumers)
            .mapToObj(i -> new Metrics())
            .collect(Collectors.toUnmodifiableList());
    var producerMetrics =
        IntStream.range(0, param.producers)
            .mapToObj(i -> new Metrics())
            .collect(Collectors.toUnmodifiableList());

    var manager = new Manager(param, producerMetrics, consumerMetrics);
    var tracker = new Tracker(producerMetrics, consumerMetrics, manager);
    var groupId = "groupId-" + System.currentTimeMillis();
    try (var threadPool =
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
                                    .configs(param.consumerProps())
                                    .consumerRebalanceListener(
                                        ignore -> manager.countDownGetAssignment())
                                    .build(),
                                consumerMetrics.get(i),
                                manager))
                    .collect(Collectors.toUnmodifiableList()))
            .executors(
                IntStream.range(0, param.producers)
                    .mapToObj(
                        i ->
                            producerExecutor(
                                Producer.builder()
                                    .configs(param.producerProps())
                                    .partitionClassName(param.partitioner)
                                    .build(),
                                Producer.builder()
                                    .configs(param.producerProps())
                                    .partitionClassName(param.partitioner)
                                    .buildTransactional(),
                                param,
                                producerMetrics.get(i),
                                partitions,
                                manager))
                    .collect(Collectors.toUnmodifiableList()))
            .executor(tracker)
            .executor((param.createCSV) ? new FileWriter(manager, tracker) : () -> State.DONE)
            .build()) {
      threadPool.waitAll();
      return new Result(param.topic);
    }
  }

  static Executor consumerExecutor(
      Consumer<byte[], byte[]> consumer, BiConsumer<Long, Long> observer, Manager manager) {
    return new Executor() {
      @Override
      public State execute() {
        try {
          consumer
              .poll(Duration.ofSeconds(10))
              .forEach(
                  record -> {
                    // record ene-to-end latency, and record input byte (header and timestamp size
                    // excluded)
                    observer.accept(
                        System.currentTimeMillis() - record.timestamp(),
                        (long) record.serializedKeySize() + record.serializedValueSize());
                  });
          // Consumer reached the record upperbound or consumed all the record producer produced.
          return manager.consumedDone() ? State.DONE : State.RUNNING;
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

  static Executor producerExecutor(
      Producer<byte[], byte[]> producer,
      TransactionalProducer<byte[], byte[]> transactionalProducer,
      Argument param,
      BiConsumer<Long, Long> observer,
      List<Integer> partitions,
      Manager manager) {
    return new Executor() {
      @Override
      public State execute() throws InterruptedException {
        // Wait for all consumers get assignment.
        manager.awaitPartitionAssignment();
        var rand = new Random();
        // Do transactional send.
        if (param.transaction()) {
          var senders =
              IntStream.range(0, param.transactionSize)
                  .mapToObj(i -> manager.payload())
                  .filter(Optional::isPresent)
                  .map(
                      p ->
                          producer
                              .sender()
                              .topic(param.topic)
                              .partition(partitions.get(rand.nextInt(partitions.size())))
                              .key(manager.getKey())
                              .value(p.get())
                              .timestamp(System.currentTimeMillis()))
                  .collect(Collectors.toList());

          // No records to send
          if (senders.isEmpty()) return State.DONE;
          transactionalProducer
              .transaction(senders)
              .forEach(
                  future ->
                      future.whenComplete(
                          (m, e) ->
                              observer.accept(
                                  System.currentTimeMillis() - m.timestamp(),
                                  m.serializedValueSize())));
        } else {
          var payload = manager.payload();
          if (payload.isEmpty()) return State.DONE;

          long start = System.currentTimeMillis();
          producer
              .sender()
              .topic(param.topic)
              .partition(partitions.get(rand.nextInt(partitions.size())))
              .key(manager.getKey())
              .value(payload.get())
              .timestamp(start)
              .run()
              .whenComplete(
                  (m, e) ->
                      observer.accept(System.currentTimeMillis() - start, m.serializedValueSize()));
        }
        return State.RUNNING;
      }

      @Override
      public void close() {
        try {
          producer.close();
        } finally {
          manager.producerClosed();
        }
      }
    };
  }

  // visible for test
  static List<Integer> partition(Argument param, TopicAdmin topicAdmin) {
    if (positiveSpecifyBroker(param)) {
      return topicAdmin
          .partitionsOfBrokers(Set.of(param.topic), new HashSet<>(param.specifyBroker))
          .stream()
          .map(TopicPartition::partition)
          .collect(Collectors.toList());
    } else return List.of(-1);
  }

  private static boolean positiveSpecifyBroker(Argument param) {
    return param.specifyBroker.stream().allMatch(broker -> broker >= 0);
  }

  public static class Argument extends org.astraea.argument.Argument {

    @Parameter(
        names = {"--topic"},
        description = "String: topic name",
        validateWith = NonEmptyStringField.class)
    String topic = "testPerformance-" + System.currentTimeMillis();

    @Parameter(
        names = {"--partitions"},
        description = "Integer: number of partitions to create the topic",
        validateWith = PositiveLongField.class)
    int partitions = 1;

    @Parameter(
        names = {"--replicas"},
        description = "Integer: number of replica to create the topic",
        validateWith = PositiveShortField.class,
        converter = PositiveShortField.class)
    short replicas = 1;

    @Parameter(
        names = {"--producers"},
        description = "Integer: number of producers to produce records",
        validateWith = PositiveShortField.class,
        converter = PositiveShortField.class)
    int producers = 1;

    @Parameter(
        names = {"--consumers"},
        description = "Integer: number of consumers to consume records",
        validateWith = NonNegativeShortField.class,
        converter = NonNegativeShortField.class)
    int consumers = 1;

    @Parameter(
        names = {"--run.until"},
        description =
            "Run until number of records are produced and consumed or until duration meets."
                + " The duration formats accepted are (a number) + (a time unit)."
                + " The time units can be \"days\", \"day\", \"h\", \"m\", \"s\", \"ms\","
                + " \"us\", \"ns\"",
        validateWith = ExeTime.Field.class,
        converter = ExeTime.Field.class)
    ExeTime exeTime = ExeTime.of("1000records");

    @Parameter(
        names = {"--fixed.size"},
        description = "boolean: send fixed size records if this flag is set")
    boolean fixedSize = false;

    @Parameter(
        names = {"--record.size"},
        description = "DataSize: size of each record. e.g. \"500KiB\"",
        converter = DataSize.Field.class)
    DataSize recordSize = DataUnit.KiB.of(1);

    @Parameter(
        names = {"--jmx.servers"},
        description =
            "String: server to get jmx metrics <jmx_server>@<broker_id>[,<jmx_server>@<broker_id>]*",
        validateWith = NonEmptyStringField.class)
    String jmxServers = "";

    @Parameter(
        names = {"--partitioner"},
        description = "String: the full class name of the desired partitioner",
        validateWith = NonEmptyStringField.class)
    String partitioner = DefaultPartitioner.class.getName();

    @Parameter(
        names = {"--configs"},
        description = "Map: set the configuration passed to producer/partitioner",
        converter = StringMapField.class,
        validateWith = StringMapField.class)
    Map<String, String> configs = Map.of();

    public Map<String, Object> producerProps() {
      var props = props();
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression.name);
      if (!this.jmxServers.isEmpty()) props.put("jmx_servers", this.jmxServers);
      props.putAll(configs);
      return props;
    }

    public Map<String, Object> consumerProps() {
      var props = props();
      if (transaction()) props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
      return props;
    }

    @Parameter(
        names = {"--createCSV"},
        description = "create the metrics into a csv file if this flag is set")
    boolean createCSV = false;

    @Parameter(
        names = {"--compression"},
        description =
            "String: the compression algorithm used by producer. Available algorithm are none, gzip, snappy, lz4, and zstd",
        converter = CompressionField.class)
    CompressionType compression = CompressionType.NONE;

    @Parameter(
        names = {"--transaction.size"},
        description = "integer: number of records in each transaction",
        validateWith = PositiveLongField.class)
    int transactionSize = 1;

    public boolean transaction() {
      return transactionSize > 1;
    }

    @Parameter(
        names = {"--key.distribution"},
        description =
            "String: Distribution name. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: uniform",
        converter = Distribution.DistributionField.class)
    Distribution distribution = Distribution.uniform();

    @Parameter(
        names = {"--specify.broker"},
        description =
            "String: Used with SpecifyBrokerPartitioner to specify the brokers that partitioner can send.",
        validateWith = NonEmptyStringField.class)
    List<Integer> specifyBroker = List.of(-1);
  }

  public static class Result {
    private final String topicName;

    private Result(String topicName) {
      this.topicName = topicName;
    }

    public String topicName() {
      return topicName;
    }
  }
}
