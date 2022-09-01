/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.performance;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Compression;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.CompressionField;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.NonEmptyStringField;
import org.astraea.app.argument.NonNegativeShortField;
import org.astraea.app.argument.PathField;
import org.astraea.app.argument.PositiveIntegerListField;
import org.astraea.app.argument.PositiveLongField;
import org.astraea.app.argument.PositiveShortField;
import org.astraea.app.argument.PositiveShortListField;
import org.astraea.app.argument.StringListField;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Isolation;
import org.astraea.app.producer.Producer;

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
    execute(org.astraea.app.argument.Argument.parse(new Argument(), args));
  }

  private static DataSupplier dataSupplier(Performance.Argument argument) {
    return DataSupplier.of(
        argument.exeTime,
        argument.keyDistributionType.create(10000),
        argument.keyDistributionType.create(argument.keySize.measurement(DataUnit.Byte).intValue()),
        argument.valueDistributionType.create(10000),
        argument.valueDistributionType.create(
            argument.valueSize.measurement(DataUnit.Byte).intValue()),
        argument.throughput);
  }

  public static List<String> execute(final Argument param)
      throws InterruptedException, IOException {
    var topicSet = new HashSet<>(param.topics);
    // always try to init topic even though it may be existent already.
    param.initTopics();

    var latestOffsets = param.lastOffsets();

    var producerThreads =
        ProducerThread.create(
            param.topics,
            param.transactionSize,
            dataSupplier(param),
            param.partitionSupplier(),
            param.producers,
            param::createProducer);
    var consumerThreads =
        ConsumerThread.create(
            param.consumers,
            listener ->
                Consumer.forTopics(topicSet)
                    .bootstrapServers(param.bootstrapServers())
                    .groupId(param.groupId)
                    .configs(param.configs())
                    .isolation(param.isolation())
                    .seek(latestOffsets)
                    .consumerRebalanceListener(listener)
                    .build());

    Supplier<List<ProducerThread.Report>> producerReporter =
        () ->
            producerThreads.stream()
                .map(ProducerThread::report)
                .collect(Collectors.toUnmodifiableList());
    Supplier<List<ConsumerThread.Report>> consumerReporter =
        () ->
            consumerThreads.stream()
                .map(ConsumerThread::report)
                .collect(Collectors.toUnmodifiableList());

    var tracker = TrackerThread.create(producerReporter, consumerReporter, param.exeTime);

    Optional<Runnable> fileWriter =
        param.CSVPath == null
            ? Optional.empty()
            : Optional.of(
                ReportFormat.createFileWriter(
                    param.reportFormat,
                    param.CSVPath,
                    () -> consumerThreads.stream().allMatch(AbstractThread::closed),
                    () -> producerThreads.stream().allMatch(AbstractThread::closed),
                    producerReporter,
                    consumerReporter));

    var fileWriterFuture =
        fileWriter.map(CompletableFuture::runAsync).orElse(CompletableFuture.completedFuture(null));

    var chaos =
        param.chaosDuration == null
            ? CompletableFuture.completedFuture(null)
            : CompletableFuture.runAsync(
                () -> {
                  while (!consumerThreads.stream().allMatch(AbstractThread::closed)) {
                    var thread =
                        consumerThreads.get((int) (Math.random() * consumerThreads.size()));
                    thread.unsubscribe();
                    Utils.sleep(param.chaosDuration);
                    thread.resubscribe();
                  }
                });

    CompletableFuture.runAsync(
        () -> {
          producerThreads.forEach(AbstractThread::waitForDone);
          // wait until all records are read already
          while (true) {
            var offsets =
                Report.maxOffsets(
                    producerThreads.stream()
                        .map(ProducerThread::report)
                        .collect(Collectors.toUnmodifiableList()));
            if (offsets.entrySet().stream()
                .allMatch(
                    e ->
                        consumerReporter.get().stream()
                            .anyMatch(r -> r.offset(e.getKey()) >= e.getValue()))) break;
            Utils.sleep(Duration.ofSeconds(2));
          }
          consumerThreads.forEach(AbstractThread::close);
        });
    consumerThreads.forEach(AbstractThread::waitForDone);
    tracker.waitForDone();
    fileWriterFuture.join();
    chaos.join();
    return param.topics;
  }

  public static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you subscribed",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        variableArity = true)
    List<String> topics = List.of("testPerformance-" + System.currentTimeMillis());

    void initTopics() {
      var topicPattern = topicPattern();
      try (var admin = Admin.of(configs())) {
        topicPattern.forEach(
            (topic, pr) -> {
              pr.forEach(
                  (p, r) ->
                      Utils.waitFor(
                          () -> {
                            admin
                                .creator()
                                .topic(topic)
                                .numberOfPartitions(p)
                                .numberOfReplicas(r)
                                .create();
                            return true;
                          },
                          Duration.ofSeconds(30)));
            });
        Utils.waitFor(() -> admin.topicNames().containsAll(topics));
      }
    }

    Map<String, Map<Integer, Short>> topicPattern() {
      Map<String, Map<Integer, Short>> pattern = new HashMap<>();
      if (partitions.size() == 1 && replicas.size() == 1) {
        topics.forEach(
            topic -> pattern.putIfAbsent(topic, Map.of(partitions.get(0), replicas.get(0))));
      } else if (topics.size() == partitions.size() && topics.size() == replicas.size()) {
        topics.forEach(
            topic -> {
              var index = topics.indexOf(topic);
              pattern.putIfAbsent(topic, Map.of(partitions.get(index), replicas.get(index)));
            });
      } else {
        throw new ParameterException(
            "the number of parameters in --partitions and --replicas doesn't match");
      }
      return pattern;
    }

    Map<TopicPartition, Long> lastOffsets() {
      try (var admin = Admin.of(configs())) {
        // the slow zk causes unknown error, so we have to wait it.
        return Utils.waitForNonNull(
            () ->
                admin.offsets(new HashSet<>(topics)).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().latest())),
            Duration.ofSeconds(30));
      }
    }

    @Parameter(
        names = {"--partitions"},
        description = "List<Integer>: number of partitions to create the topics",
        validateWith = PositiveIntegerListField.class,
        listConverter = PositiveIntegerListField.class,
        variableArity = true)
    List<Integer> partitions = List.of(1);

    @Parameter(
        names = {"--replicas"},
        description = "List<Short>: number of replica to create the topics",
        validateWith = PositiveShortListField.class,
        listConverter = PositiveShortListField.class,
        variableArity = true)
    List<Short> replicas = List.of((short) 1);

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
        names = {"--partitioner"},
        description = "String: the full class name of the desired partitioner",
        validateWith = NonEmptyStringField.class)
    String partitioner = null;

    @Parameter(
        names = {"--compression"},
        description =
            "String: the compression algorithm used by producer. Available algorithm are none, gzip, snappy, lz4, and zstd",
        converter = CompressionField.class)
    Compression compression = Compression.NONE;

    @Parameter(
        names = {"--transaction.size"},
        description =
            "integer: number of records in each transaction. the value larger than 1 means the producer works for transaction",
        validateWith = PositiveLongField.class)
    int transactionSize = 1;

    Isolation isolation() {
      return transactionSize > 1 ? Isolation.READ_COMMITTED : Isolation.READ_UNCOMMITTED;
    }

    Producer<byte[], byte[]> createProducer() {
      return transactionSize > 1
          ? Producer.builder()
              .configs(configs())
              .bootstrapServers(bootstrapServers())
              .compression(compression)
              .partitionClassName(partitioner)
              .buildTransactional()
          : Producer.builder()
              .configs(configs())
              .bootstrapServers(bootstrapServers())
              .compression(compression)
              .partitionClassName(partitioner)
              .build();
    }

    @Parameter(
        names = {"--key.size"},
        description = "DataSize of the key. Default: 4Byte",
        converter = DataSize.Field.class)
    DataSize keySize = DataSize.Byte.of(4);

    @Parameter(
        names = {"--value.size"},
        description = "DataSize of the value. Default: 1KiB",
        converter = DataSize.Field.class)
    DataSize valueSize = DataSize.KiB.of(1);

    @Parameter(
        names = {"--key.distribution"},
        description =
            "Distribution name for key and key size. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: uniform",
        converter = DistributionType.DistributionTypeField.class)
    DistributionType keyDistributionType = DistributionType.UNIFORM;

    @Parameter(
        names = {"--value.distribution"},
        description =
            "Distribution name for value and value size. Available distribution names: \"uniform\", \"zipfian\", \"latest\", \"fixed\". Default: uniform",
        converter = DistributionType.DistributionTypeField.class)
    DistributionType valueDistributionType = DistributionType.UNIFORM;

    @Parameter(
        names = {"--specify.broker"},
        description =
            "String: Used with SpecifyBrokerPartitioner to specify the brokers that partitioner can send.",
        validateWith = NonEmptyStringField.class)
    List<Integer> specifyBroker = List.of();

    Supplier<Integer> partitionSupplier() {
      if (specifyBroker.isEmpty()) return () -> -1;
      try (var admin = Admin.of(configs())) {
        var partitions =
            admin.partitions(new HashSet<>(topics), new HashSet<>(specifyBroker)).values().stream()
                .flatMap(Collection::stream)
                .map(TopicPartition::partition)
                .collect(Collectors.toUnmodifiableList());
        return () -> partitions.get((int) (Math.random() * partitions.size()));
      }
    }

    // replace DataSize by DataRate (see https://github.com/skiptests/astraea/issues/488)
    @Parameter(
        names = {"--throughput"},
        description =
            "dataRate: size output/timeUnit. Default: second. e.g. \"500KiB/second\", \"100 MB/PT-10S\"",
        converter = DataRate.Field.class)
    DataRate throughput = DataRate.GiB.of(500).perSecond();

    @Parameter(
        names = {"--report.path"},
        description = "String: A path to place the report. Default: (no report)",
        converter = PathField.class)
    Path CSVPath = null;

    @Parameter(
        names = {"--report.format"},
        description = "Output format for the report",
        converter = ReportFormat.ReportFormatConverter.class)
    ReportFormat reportFormat = ReportFormat.CSV;

    @Parameter(
        names = {"--chaos.frequency"},
        description =
            "time to run the chaos monkey. It will kill consumer arbitrarily. There is no monkey by default",
        validateWith = DurationField.class,
        converter = DurationField.class)
    Duration chaosDuration = null;

    @Parameter(
        names = {"--group.id"},
        description = "Consumer group id",
        validateWith = NonEmptyStringField.class)
    String groupId = "groupId-" + System.currentTimeMillis();
  }
}
