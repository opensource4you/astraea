package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.CompressionType;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgumentWithPropFile;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.producer.Producer;
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
    execute(ArgumentUtil.parseArgument(new Argument(), args));
  }

  public static Future<String> execute(final Argument param)
      throws InterruptedException, IOException, ExecutionException {
    List<Integer> partitions;
    var future = new CompletableFuture<String>();
    try (var topicAdmin = TopicAdmin.of(param.props())) {
      topicAdmin
          .creator()
          .numberOfReplicas(param.replicas)
          .numberOfPartitions(param.partitions)
          .topic(param.topic)
          .create();

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
    Collection<ThreadPool.Executor> fileWriter =
        (param.createCSV) ? List.of(new FileWriter(manager, tracker)) : List.of();
    var groupId = "groupId-" + System.currentTimeMillis();
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
                                    .configs(param.props())
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
                                param,
                                producerMetrics.get(i),
                                partitions,
                                manager))
                    .collect(Collectors.toUnmodifiableList()))
            .executor(tracker)
            .executors(fileWriter)
            .build()) {
      threadPool.waitAll();
      future.complete(param.topic);
      return future;
    }
  }

  static ThreadPool.Executor consumerExecutor(
      Consumer<byte[], byte[]> consumer, BiConsumer<Long, Long> observer, Manager manager) {
    return new ThreadPool.Executor() {
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

  static ThreadPool.Executor producerExecutor(
      Producer<byte[], byte[]> producer,
      Argument param,
      BiConsumer<Long, Long> observer,
      List<Integer> partitions,
      Manager manager) {
    return new ThreadPool.Executor() {
      @Override
      public State execute() throws InterruptedException {
        // Wait for all consumers get assignment.
        manager.awaitPartitionAssignment();
        var rand = new Random();
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
          .map(topicPartition -> topicPartition.partition())
          .collect(Collectors.toList());
    } else return List.of(-1);
  }

  private static boolean positiveSpecifyBroker(Argument param) {
    return param.specifyBroker.stream().allMatch(broker -> broker >= 0);
  }

  public static class Argument extends BasicArgumentWithPropFile {

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
        names = {"--run.until"},
        description =
            "Run until number of records are produced and consumed or until duration meets."
                + " The duration formats accepted are (a number) + (a time unit)."
                + " The time units can be \"days\", \"day\", \"h\", \"m\", \"s\", \"ms\","
                + " \"us\", \"ns\"",
        validateWith = ExeTime.Validator.class,
        converter = ExeTime.Converter.class)
    ExeTime exeTime = ExeTime.of("1000records");

    @Parameter(
        names = {"--fixed.size"},
        description = "boolean: send fixed size records if this flag is set")
    boolean fixedSize = false;

    @Parameter(
        names = {"--record.size"},
        description = "DataSize: size of each record. e.g. \"500KiB\"",
        converter = DataSize.Converter.class)
    DataSize recordSize = DataUnit.KiB.of(1);

    @Parameter(
        names = {"--jmx.servers"},
        description =
            "String: server to get jmx metrics <jmx_server>@<broker_id>[,<jmx_server>@<broker_id>]*",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String jmxServers = "";

    @Parameter(
        names = {"--partitioner"},
        description = "String: the full class name of the desired partitioner",
        validateWith = ArgumentUtil.NotEmptyString.class)
    String partitioner = DefaultPartitioner.class.getName();

    public Map<String, Object> producerProps() {
      var props = props();
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression.name);
      if (!this.jmxServers.isEmpty()) props.put("jmx_servers", this.jmxServers);
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
        converter = CompressionArgument.class)
    CompressionType compression = CompressionType.NONE;

    @Parameter(
        names = {"--key.distribution"},
        description =
            "String: Distribution name. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: uniform",
        converter = Distribution.DistributionConverter.class)
    Distribution distribution = Distribution.uniform();

    @Parameter(
        names = {"--specify.broker"},
        description =
            "String: Used with SpecifyBrokerPartitioner to specify the brokers that partitioner can send.",
        validateWith = ArgumentUtil.NotEmptyString.class)
    List<Integer> specifyBroker = List.of(-1);
  }

  static class CompressionArgument implements IStringConverter<CompressionType> {

    /**
     * @param value Name of compression type. Accept lower-case name only ("none", "gzip", "snappy",
     *     "lz4", "zstd").
     */
    @Override
    public CompressionType convert(String value) {
      try {
        // `CompressionType#forName` accept lower-case name only.
        return CompressionType.forName(value);
      } catch (IllegalArgumentException e) {
        throw new ParameterException(
            "the "
                + value
                + " is unsupported. The supported algorithms are "
                + Stream.of(CompressionType.values())
                    .map(CompressionType::name)
                    .collect(Collectors.joining(",")));
      }
    }
  }
}
