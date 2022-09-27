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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Compression;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.DurationField;
import org.astraea.common.argument.NonEmptyStringField;
import org.astraea.common.argument.NonNegativeShortField;
import org.astraea.common.argument.PathField;
import org.astraea.common.argument.PositiveIntegerField;
import org.astraea.common.argument.PositiveIntegerListField;
import org.astraea.common.argument.PositiveLongField;
import org.astraea.common.argument.PositiveShortField;
import org.astraea.common.argument.StringListField;
import org.astraea.common.argument.TopicPartitionField;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.Isolation;
import org.astraea.common.partitioner.Dispatcher;
import org.astraea.common.producer.Acks;
import org.astraea.common.producer.Producer;

/** see docs/performance_benchmark.md for man page */
public class Performance {
  /** Used in Automation, to achieve the end of one Performance and then start another. */
  public static void main(String[] args) throws InterruptedException, IOException {
    execute(Performance.Argument.parse(new Argument(), args));
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
    // always try to init topic even though it may be existent already.
    System.out.println("checking topics: " + String.join(",", param.topics));
    param.checkTopics();

    System.out.println("seeking offsets");
    var latestOffsets = param.lastOffsets();

    System.out.println("creating threads");
    var producerThreads =
        ProducerThread.create(
            param.transactionSize,
            dataSupplier(param),
            param.topicPartitionSelector(),
            param.producers,
            param::createProducer,
            param.interdependent);
    var consumerThreads =
        ConsumerThread.create(
            param.consumers,
            listener ->
                Consumer.forTopics(new HashSet<>(param.topics))
                    .bootstrapServers(param.bootstrapServers())
                    .groupId(param.groupId)
                    .configs(param.configs())
                    .isolation(param.isolation())
                    .seek(latestOffsets)
                    .consumerRebalanceListener(listener)
                    .build());

    System.out.println("creating tracker");
    var tracker =
        TrackerThread.create(
            () -> producerThreads.stream().allMatch(AbstractThread::closed),
            () -> consumerThreads.stream().allMatch(AbstractThread::closed));

    Optional<Runnable> fileWriter =
        param.CSVPath == null
            ? Optional.empty()
            : Optional.of(
                ReportFormat.createFileWriter(
                    param.reportFormat,
                    param.CSVPath,
                    () -> consumerThreads.stream().allMatch(AbstractThread::closed),
                    () -> producerThreads.stream().allMatch(AbstractThread::closed)));

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
          var last = 0L;
          var lastChange = System.currentTimeMillis();
          while (true) {
            var current = Report.recordsConsumedTotal();
            if (current != last) {
              last = current;
              lastChange = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - lastChange >= param.readIdle.toMillis()) {
              consumerThreads.forEach(AbstractThread::close);
              return;
            }
            Utils.sleep(Duration.ofSeconds(1));
          }
        });
    consumerThreads.forEach(AbstractThread::waitForDone);
    tracker.waitForDone();
    fileWriterFuture.join();
    chaos.join();
    return param.topics;
  }

  public static class Argument extends org.astraea.common.argument.Argument {

    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you subscribed",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;

    void checkTopics() {
      try (var admin = Admin.of(configs())) {
        var existentTopics = admin.topicNames();
        var nonexistent =
            topics.stream().filter(t -> !existentTopics.contains(t)).collect(Collectors.toSet());
        if (!nonexistent.isEmpty())
          throw new IllegalArgumentException(nonexistent + " are not existent");
      }
    }

    Map<TopicPartition, Long> lastOffsets() {
      try (var admin = Admin.of(configs())) {
        // the slow zk causes unknown error, so we have to wait it.
        return Utils.waitForNonNull(
            () ->
                admin.partitions(new HashSet<>(topics)).stream()
                    .collect(Collectors.toMap(Partition::topicPartition, Partition::latestOffset)),
            Duration.ofSeconds(30));
      }
    }

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

    String partitioner() {
      // The given partitioner should be Astraea Dispatcher when interdependent is set
      if (this.interdependent > 1) {
        try {
          if (this.partitioner == null
              || !Dispatcher.class.isAssignableFrom(Class.forName(this.partitioner))) {
            throw new ParameterException(
                "The given partitioner \""
                    + this.partitioner
                    + "\" is not a subclass of Astraea Dispatcher");
          }
        } catch (ClassNotFoundException e) {
          throw new ParameterException(
              "The given partitioner \"" + this.partitioner + "\" was not found.");
        }
      }
      if (this.partitioner != null) {
        if (!this.specifyBrokers.isEmpty())
          throw new IllegalArgumentException(
              "--specify.brokers can't be used in conjunction with partitioner");
        if (!this.specifyPartitions.isEmpty())
          throw new IllegalArgumentException(
              "--specify.partitions can't be used in conjunction with partitioner");
      }
      return this.partitioner;
    }

    @Parameter(
        names = {"--compression"},
        description =
            "String: the compression algorithm used by producer. Available algorithm are none, gzip, snappy, lz4, and zstd",
        converter = Compression.Field.class)
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
              .partitionClassName(partitioner())
              .acks(acks)
              .buildTransactional()
          : Producer.builder()
              .configs(configs())
              .bootstrapServers(bootstrapServers())
              .compression(compression)
              .partitionClassName(partitioner())
              .acks(acks)
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
        names = {"--specify.brokers"},
        description =
            "String: The broker IDs to send to if the topic has partition on that broker.",
        validateWith = PositiveIntegerListField.class)
    List<Integer> specifyBrokers = List.of();

    @Parameter(
        names = {"--specify.partitions"},
        description =
            "String: A list ot topic-partition pairs, this list specify the send targets in "
                + "partition level. This argument can't be use in conjunction with `specify.brokers`, `topics` or `partitioner`.",
        converter = TopicPartitionField.class)
    List<TopicPartition> specifyPartitions = List.of();

    /** @return a supplier that randomly return a sending target */
    Supplier<TopicPartition> topicPartitionSelector() {
      var specifiedByBroker = !specifyBrokers.isEmpty();
      var specifiedByPartition = !specifyPartitions.isEmpty();
      if (specifiedByBroker && specifiedByPartition)
        throw new IllegalArgumentException(
            "`--specify.partitions` can't be used in conjunction with `--specify.brokers`");
      else if (specifiedByBroker) {
        try (Admin admin = Admin.of(configs())) {
          final var selections =
              admin.replicas(Set.copyOf(topics)).values().stream()
                  .flatMap(Collection::stream)
                  .filter(ReplicaInfo::isLeader)
                  .filter(replica -> specifyBrokers.contains(replica.nodeInfo().id()))
                  .map(replica -> TopicPartition.of(replica.topic(), replica.partition()))
                  .distinct()
                  .collect(Collectors.toUnmodifiableList());

          if (selections.isEmpty())
            throw new IllegalArgumentException(
                "No partition match the specify.brokers requirement");

          return () -> selections.get(ThreadLocalRandom.current().nextInt(selections.size()));
        }
      } else if (specifiedByPartition) {
        // specify.partitions can't be use in conjunction with partitioner or topics
        if (partitioner != null)
          throw new IllegalArgumentException(
              "--specify.partitions can't be used in conjunction with partitioner");
        // sanity check, ensure all specified partitions are existed
        try (Admin admin = Admin.of(configs())) {
          var allTopics = admin.topicNames();
          var allTopicPartitions =
              admin
                  .replicas(
                      specifyPartitions.stream()
                          .map(TopicPartition::topic)
                          .filter(allTopics::contains)
                          .collect(Collectors.toUnmodifiableSet()))
                  .keySet();
          var notExist =
              specifyPartitions.stream()
                  .filter(tp -> !allTopicPartitions.contains(tp))
                  .collect(Collectors.toUnmodifiableSet());
          if (!notExist.isEmpty())
            throw new IllegalArgumentException(
                "The following topic/partitions are nonexistent in the cluster: " + notExist);
        }

        final var selection =
            specifyPartitions.stream().distinct().collect(Collectors.toUnmodifiableList());
        return () -> selection.get(ThreadLocalRandom.current().nextInt(selection.size()));
      } else {
        final var selection =
            topics.stream()
                .map(topic -> TopicPartition.of(topic, -1))
                .distinct()
                .collect(Collectors.toUnmodifiableList());
        return () -> selection.get(ThreadLocalRandom.current().nextInt(selection.size()));
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

    @Parameter(
        names = {"--acks"},
        description = "How many replicas should be synced when producing records.",
        converter = Acks.Field.class)
    Acks acks = Acks.ISRS;

    @Parameter(
        names = {"--read.idle"},
        description =
            "Perf will close all read processes if it can't get more data in this duration",
        converter = DurationField.class)
    Duration readIdle = Duration.ofSeconds(2);

    @Parameter(
        names = {"--interdependent.size"},
        description =
            "Integer: the number of records sending to the same partition (Note: this parameter only works for Astraea partitioner)",
        validateWith = PositiveIntegerField.class)
    int interdependent = 1;
  }
}
