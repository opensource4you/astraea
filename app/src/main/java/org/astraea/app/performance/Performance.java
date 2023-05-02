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
import com.beust.jcommander.converters.LongConverter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.argument.DataRateField;
import org.astraea.app.argument.DataSizeField;
import org.astraea.app.argument.DistributionTypeField;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.DurationMapField;
import org.astraea.app.argument.NonEmptyStringField;
import org.astraea.app.argument.NonNegativeShortField;
import org.astraea.app.argument.PathField;
import org.astraea.app.argument.PatternField;
import org.astraea.app.argument.PositiveIntegerField;
import org.astraea.app.argument.PositiveIntegerListField;
import org.astraea.app.argument.PositiveLongField;
import org.astraea.app.argument.StringListField;
import org.astraea.app.argument.StringMapField;
import org.astraea.app.argument.TopicPartitionDataRateMapField;
import org.astraea.app.argument.TopicPartitionField;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.DistributionType;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.partitioner.Partitioner;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.ProducerConfigs;
import org.astraea.common.producer.Record;

/** see docs/performance_benchmark.md for man page */
public class Performance {
  /** Used in Automation, to achieve the end of one Performance and then start another. */
  public static void main(String[] args) {
    execute(Performance.Argument.parse(new Argument(), args));
  }

  public static List<String> execute(final Argument param) {
    var blockingQueues =
        IntStream.range(0, param.producers)
            .mapToObj(i -> new ArrayBlockingQueue<List<Record<byte[], byte[]>>>(3000))
            .collect(Collectors.toUnmodifiableList());
    // ensure topics are existent
    System.out.println("checking topics: " + String.join(",", param.topics));
    param.checkTopics();

    System.out.println("seeking offsets");
    var latestOffsets = param.lastOffsets();

    System.out.println("creating threads");
    var producerThreads =
        ProducerThread.create(blockingQueues, param::createProducer, param.interdependent);
    var consumerThreads =
        param.monkeys != null
            ? Collections.synchronizedList(new ArrayList<>(consumers(param, latestOffsets)))
            : consumers(param, latestOffsets);

    System.out.println("creating data generator");
    var dataGenerator = DataGenerator.of(blockingQueues, param.topicPartitionSelector(), param);

    System.out.println("creating tracker");
    var tracker =
        TrackerThread.create(
            () -> producerThreads.stream().allMatch(AbstractThread::closed),
            () -> consumerThreads.stream().allMatch(AbstractThread::closed));

    var fileWriterTask =
        CompletableFuture.completedFuture(
                param.CSVPath == null
                    ? (Runnable) (() -> {})
                    : ReportFormat.createFileWriter(
                        param.reportFormat,
                        param.CSVPath,
                        () -> consumerThreads.stream().allMatch(AbstractThread::closed),
                        () -> producerThreads.stream().allMatch(AbstractThread::closed)))
            .thenAcceptAsync(Runnable::run);

    var monkeys = MonkeyThread.play(consumerThreads, param);

    CompletableFuture.runAsync(
        () -> {
          dataGenerator.waitForDone();
          var last = 0L;
          var lastChange = System.currentTimeMillis();
          while (true) {
            var current = Report.recordsConsumedTotal();

            if (blockingQueues.stream().allMatch(Collection::isEmpty)) {
              var unfinishedProducers =
                  producerThreads.stream()
                      .filter(p -> !p.closed())
                      .collect(Collectors.toUnmodifiableList());
              unfinishedProducers.forEach(AbstractThread::close);
            }

            if (current != last) {
              last = current;
              lastChange = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - lastChange >= param.readIdle.toMillis()) {
              consumerThreads.forEach(AbstractThread::close);
              monkeys.forEach(AbstractThread::close);
            }
            if (consumerThreads.stream().allMatch(AbstractThread::closed)
                && monkeys.stream().allMatch(AbstractThread::closed)
                && producerThreads.stream().allMatch(AbstractThread::closed)) return;
            Utils.sleep(Duration.ofSeconds(1));
          }
        });
    producerThreads.forEach(AbstractThread::waitForDone);
    monkeys.forEach(AbstractThread::waitForDone);
    consumerThreads.forEach(AbstractThread::waitForDone);
    tracker.waitForDone();
    fileWriterTask.join();
    return param.topics;
  }

  static List<ConsumerThread> consumers(Argument param, Map<TopicPartition, Long> latestOffsets) {
    return ConsumerThread.create(
        param.consumers,
        (clientId, listener) ->
            (param.pattern == null
                    ? Consumer.forTopics(Set.copyOf(param.topics))
                    : Consumer.forTopics(param.pattern))
                .configs(param.configs())
                .config(
                    ConsumerConfigs.ISOLATION_LEVEL_CONFIG,
                    param.transactionSize > 1
                        ? ConsumerConfigs.ISOLATION_LEVEL_COMMITTED
                        : ConsumerConfigs.ISOLATION_LEVEL_UNCOMMITTED)
                .bootstrapServers(param.bootstrapServers())
                .config(ConsumerConfigs.GROUP_ID_CONFIG, param.groupId)
                .seek(latestOffsets)
                .consumerRebalanceListener(listener)
                .config(ConsumerConfigs.CLIENT_ID_CONFIG, clientId)
                .build());
  }

  public static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you subscribed",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;

    @Parameter(
        names = {"--pattern"},
        description = "Pattern: topic pattern(s) which consumers subscribed",
        converter = PatternField.class)
    Pattern pattern = null;

    void checkTopics() {
      try (var admin = Admin.of(configs())) {
        var existentTopics = admin.topicNames(false).toCompletableFuture().join();
        var nonexistent =
            topics.stream().filter(t -> !existentTopics.contains(t)).collect(Collectors.toSet());
        if (!nonexistent.isEmpty())
          throw new IllegalArgumentException(nonexistent + " are not existent");
      }
    }

    Map<TopicPartition, Long> lastOffsets() {
      try (var admin = Admin.of(configs())) {
        return admin.partitions(Set.copyOf(topics)).toCompletableFuture().join().stream()
            .collect(Collectors.toMap(Partition::topicPartition, Partition::latestOffset));
      }
    }

    @Parameter(
        names = {"--producers"},
        description = "Integer: number of producers to produce records",
        validateWith = NonNegativeShortField.class,
        converter = NonNegativeShortField.class)
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
      // The given partitioner should be Astraea Partitioner when interdependent is set
      if (this.interdependent > 1) {
        try {
          if (this.partitioner == null
              || !Partitioner.class.isAssignableFrom(Class.forName(this.partitioner))) {
            throw new ParameterException(
                "The given partitioner \""
                    + this.partitioner
                    + "\" is not a subclass of Astraea Partitioner");
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
        names = {"--transaction.size"},
        description =
            "integer: number of records in each transaction. the value larger than 1 means the producer works for transaction",
        validateWith = PositiveLongField.class)
    int transactionSize = 1;

    Producer<byte[], byte[]> createProducer() {
      return transactionSize > 1
          ? Producer.builder()
              .configs(configs())
              .bootstrapServers(bootstrapServers())
              .config(ProducerConfigs.PARTITIONER_CLASS_CONFIG, partitioner())
              .buildTransactional()
          : Producer.builder()
              .configs(configs())
              .bootstrapServers(bootstrapServers())
              .config(ProducerConfigs.PARTITIONER_CLASS_CONFIG, partitioner())
              .build();
    }

    @Parameter(
        names = {"--record.key.table.seed"},
        description =
            "The random seed for internal record key candidate generation. "
                + "If one wants to run performance tool at multiple instances, consider using a common seed for all performance tools. "
                + "Doing so guarantee all instances send record against the same key/value content lookup table. "
                + "This might be crucial for probability distribution like Zipfian. Where only a specific key will be the hotspot.",
        converter = LongConverter.class)
    long recordKeyTableSeed = ThreadLocalRandom.current().nextLong();

    @Parameter(
        names = {"--record.value.table.seed"},
        description = "The random seed for internal record value candidate generation.",
        converter = LongConverter.class)
    long recordValueTableSeed = ThreadLocalRandom.current().nextLong();

    @Parameter(
        names = {"--key.size"},
        description = "DataSize of the key. Default: 4Byte",
        converter = DataSizeField.class)
    DataSize keySize = DataSize.Byte.of(4);

    @Parameter(
        names = {"--value.size"},
        description = "DataSize of the value. Default: 1KiB",
        converter = DataSizeField.class)
    DataSize valueSize = DataSize.KiB.of(1);

    @Parameter(
        names = {"--key.distribution"},
        description =
            "Distribution name for key content. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: uniform",
        converter = DistributionTypeField.class)
    DistributionType keyDistributionType = DistributionType.UNIFORM;

    @Parameter(
        names = {"--key.distribution.config"},
        description = "Configuration for key distribution",
        converter = StringMapField.class)
    Map<String, String> keyDistributionConfig = Map.of();

    @Parameter(
        names = {"--key.size.distribution"},
        description =
            "Distribution name for key size. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: fixed",
        converter = DistributionTypeField.class)
    DistributionType keySizeDistributionType = DistributionType.FIXED;

    @Parameter(
        names = {"--key.size.distribution.config"},
        description = "Configuration for key size distribution",
        converter = StringMapField.class)
    Map<String, String> keySizeDistributionConfig = Map.of();

    @Parameter(
        names = {"--value.distribution"},
        description =
            "Distribution name for value and value size. Available distribution names: \"uniform\", \"zipfian\", \"latest\", \"fixed\". Default: uniform",
        converter = DistributionTypeField.class)
    DistributionType valueDistributionType = DistributionType.UNIFORM;

    @Parameter(
        names = {"--value.distribution.config"},
        description = "Configuration for key distribution",
        converter = StringMapField.class)
    Map<String, String> valueDistributionConfig = Map.of();

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
    /**
     * @return a supplier that randomly return a sending target
     */
    Supplier<TopicPartition> topicPartitionSelector() {
      var specifiedByBroker = !specifyBrokers.isEmpty();
      var specifiedByPartition = !specifyPartitions.isEmpty();
      var throttle = !throttles.isEmpty();
      if (specifiedByBroker && specifiedByPartition)
        throw new IllegalArgumentException(
            "`--specify.partitions` can't be used in conjunction with `--specify.brokers`");
      else if (specifiedByBroker) {
        try (var admin = Admin.of(configs())) {
          final var selections =
              admin
                  .clusterInfo(Set.copyOf(topics))
                  .toCompletableFuture()
                  .join()
                  .replicaStream()
                  .filter(Replica::isLeader)
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
        try (var admin = Admin.of(configs())) {
          var allTopics = admin.topicNames(false).toCompletableFuture().join();
          // give the time to update metadata
          Utils.sleep(Duration.ofSeconds(1));
          var allTopicPartitions =
              admin
                  .clusterInfo(
                      specifyPartitions.stream()
                          .map(TopicPartition::topic)
                          .filter(allTopics::contains)
                          .collect(Collectors.toUnmodifiableSet()))
                  .toCompletableFuture()
                  .join()
                  .replicaStream()
                  .map(replica -> TopicPartition.of(replica.topic(), replica.partition()))
                  .collect(Collectors.toSet());
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
      } else if (throttle) {
        // TODO: The functions of throttle and select partitioner should not conflict with each
        // other
        if (partitioner != null)
          throw new IllegalArgumentException(
              "--throttle can't be used in conjunction with partitioner");
        try (var admin = Admin.of(configs())) {
          final var selection =
              admin
                  .clusterInfo(Set.copyOf(topics))
                  .toCompletableFuture()
                  .join()
                  .replicaStream()
                  .map(Replica::topicPartition)
                  .distinct()
                  .collect(Collectors.toUnmodifiableList());
          return () -> selection.get(ThreadLocalRandom.current().nextInt(selection.size()));
        }
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
        converter = DataRateField.class)
    DataRate throughput = DataRate.GiB.of(500);

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
        names = {"--monkeys"},
        description =
            "Set the frequency of chaos monkeys. Here are offering three monkeys - kill, add, unsubscribe. There is no monkey by default.",
        converter = DurationMapField.class,
        validateWith = DurationMapField.class)
    Map<String, Duration> monkeys = null;

    @Parameter(
        names = {"--group.id"},
        description = "Consumer group id",
        validateWith = NonEmptyStringField.class)
    String groupId = "groupId-" + System.currentTimeMillis();

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

    @Parameter(
        names = {"--throttle"},
        description = "Map<String, DataRate>: Set the topic-partitions and its' throttle data rate",
        converter = TopicPartitionDataRateMapField.class)
    Map<TopicPartition, DataRate> throttles = Map.of();
  }
}
