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
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Compression;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.CompressionField;
import org.astraea.app.argument.NonEmptyStringField;
import org.astraea.app.argument.NonNegativeShortField;
import org.astraea.app.argument.PathField;
import org.astraea.app.argument.PositiveIntegerField;
import org.astraea.app.argument.PositiveLongField;
import org.astraea.app.argument.PositiveShortField;
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

  public static String execute(final Argument param) throws InterruptedException, IOException {
    List<Integer> partitions;
    Map<TopicPartition, Long> latestOffsets;
    try (var topicAdmin = Admin.of(param.configs())) {
      topicAdmin
          .creator()
          .numberOfReplicas(param.replicas)
          .numberOfPartitions(param.partitions)
          .topic(param.topic)
          .create();

      Utils.waitFor(() -> topicAdmin.topicNames().contains(param.topic));
      partitions = new ArrayList<>(partition(param, topicAdmin));
      latestOffsets =
          topicAdmin.offsets(Set.of(param.topic)).entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().latest()));
    }

    var groupId = "groupId-" + System.currentTimeMillis();

    Supplier<Integer> partitionSupplier =
        () -> partitions.isEmpty() ? -1 : partitions.get((int) (Math.random() * partitions.size()));

    var producerThreads =
        ProducerThread.create(
            param.topic,
            param.transactionSize,
            dataSupplier(param),
            partitionSupplier,
            param.producers,
            param::createProducer);
    var consumerThreads =
        ConsumerThread.create(
            param.consumers,
            () ->
                Consumer.forTopics(Set.of(param.topic))
                    .bootstrapServers(param.bootstrapServers())
                    .groupId(groupId)
                    .configs(param.configs())
                    .isolation(param.isolation())
                    .seek(latestOffsets)
                    .build());

    var producerReports =
        producerThreads.stream()
            .map(ProducerThread::report)
            .collect(Collectors.toUnmodifiableList());
    var consumerReports =
        consumerThreads.stream()
            .map(ConsumerThread::report)
            .collect(Collectors.toUnmodifiableList());

    var tracker = TrackerThread.create(producerReports, consumerReports, param.exeTime);

    Optional<Runnable> fileWriter =
        param.CSVPath == null
            ? Optional.empty()
            : Optional.of(
                ReportFormat.createFileWriter(
                    param.reportFormat,
                    param.CSVPath,
                    param.exeTime,
                    () -> consumerThreads.stream().allMatch(AbstractThread::closed),
                    () -> producerThreads.stream().allMatch(AbstractThread::closed),
                    () ->
                        producerThreads.stream()
                            .map(ProducerThread::report)
                            .mapToLong(Report::records)
                            .sum(),
                    tracker));

    var fileWriterFuture =
        fileWriter.map(CompletableFuture::runAsync).orElse(CompletableFuture.completedFuture(null));

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
                        consumerReports.stream()
                            .anyMatch(r -> r.offset(e.getKey()) >= e.getValue()))) break;
            Utils.sleep(Duration.ofSeconds(2));
          }
          consumerThreads.forEach(AbstractThread::close);
        });

    consumerThreads.forEach(AbstractThread::waitForDone);
    tracker.waitForDone();
    fileWriterFuture.join();
    return param.topic;
  }

  // visible for test
  static Set<Integer> partition(Argument param, Admin topicAdmin) {
    if (positiveSpecifyBroker(param)) {
      return topicAdmin
          .partitions(Set.of(param.topic), new HashSet<>(param.specifyBroker))
          .values()
          .stream()
          .flatMap(Collection::stream)
          .map(TopicPartition::partition)
          .collect(Collectors.toSet());
    } else return Set.of(-1);
  }

  private static boolean positiveSpecifyBroker(Argument param) {
    return param.specifyBroker.stream().allMatch(broker -> broker >= 0);
  }

  public static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--topic"},
        description = "String: topic name",
        validateWith = NonEmptyStringField.class)
    String topic = "testPerformance-" + System.currentTimeMillis();

    @Parameter(
        names = {"--partitions"},
        description = "Integer: number of partitions to create the topic",
        validateWith = PositiveIntegerField.class)
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
    List<Integer> specifyBroker = List.of(-1);

    // replace DataSize by DataRate (see https://github.com/skiptests/astraea/issues/488)
    @Parameter(
        names = {"--throughput"},
        description = "dataSize: size output per second. e.g. \"500KiB\"",
        converter = DataSize.Field.class)
    DataSize throughput = DataSize.GiB.of(500);

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
  }
}
