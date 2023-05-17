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
package org.astraea.connector.perf;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.DistributionType;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.producer.RecordGenerator;
import org.astraea.connector.Definition;
import org.astraea.connector.MetadataStorage;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceRecord;
import org.astraea.connector.SourceTask;

public class PerfSource extends SourceConnector {
  static Definition THROUGHPUT_DEF =
      Definition.builder()
          .name("throughput")
          .type(Definition.Type.STRING)
          .defaultValue("100GB")
          .validator((name, value) -> DataSize.of(value.toString()))
          .documentation("the data rate (in second) of sending records")
          .build();

  static Definition KEY_DISTRIBUTION_DEF =
      Definition.builder()
          .name("key.distribution")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DistributionType.ofAlias(obj.toString()))
          .defaultValue(DistributionType.UNIFORM.alias())
          .documentation(
              "Distribution name for key and key size. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: uniform")
          .build();
  static Definition KEY_SIZE_DISTRIBUTION_DEF =
      Definition.builder()
          .name("key.size.distribution")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DistributionType.ofAlias(obj.toString()))
          .defaultValue(DistributionType.FIXED.alias())
          .documentation(
              "Distribution name for key size. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: fixed")
          .build();
  static Definition KEY_SIZE_DEF =
      Definition.builder()
          .name("key.size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue(DataSize.Byte.of(50).toString())
          .documentation(
              "the max length of key. The distribution of length is defined by "
                  + KEY_DISTRIBUTION_DEF.name())
          .build();

  static Definition VALUE_DISTRIBUTION_DEF =
      Definition.builder()
          .name("value.distribution")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DistributionType.ofAlias(obj.toString()))
          .defaultValue(DistributionType.UNIFORM.alias())
          .documentation(
              "Distribution name for value and value size. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: uniform")
          .build();
  static Definition VALUE_SIZE_DEF =
      Definition.builder()
          .name("value.size")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue(DataSize.KB.of(1).toString())
          .documentation(
              "the max length of value. The distribution of length is defined by "
                  + VALUE_DISTRIBUTION_DEF.name())
          .build();
  static Definition VALUE_SIZE_DISTRIBUTION_DEF =
      Definition.builder()
          .name("value.size.distribution")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DistributionType.ofAlias(obj.toString()))
          .defaultValue(DistributionType.FIXED.alias())
          .documentation(
              "Distribution name for value size. Available distribution names: \"fixed\" \"uniform\", \"zipfian\", \"latest\". Default: fixed")
          .build();
  static Definition BATCH_SIZE_DEF =
      Definition.builder()
          .name("batch.size")
          .type(Definition.Type.INT)
          .defaultValue(1)
          .documentation("the max length of batching messages.")
          .build();
  static Definition KEY_TABLE_SEED =
      Definition.builder()
          .name("key.table.seed")
          .type(Definition.Type.LONG)
          .defaultValue(ThreadLocalRandom.current().nextLong())
          .documentation("The random seed for internal record key candidate generation.")
          .build();
  static Definition VALUE_TABLE_SEED =
      Definition.builder()
          .name("value.table.seed")
          .type(Definition.Type.LONG)
          .defaultValue(ThreadLocalRandom.current().nextLong())
          .documentation("The random seed for internal record value candidate generation.")
          .build();

  private Configuration config;

  @Override
  protected void init(Configuration configuration, MetadataStorage storage) {
    this.config = configuration;
  }

  @Override
  protected Class<? extends SourceTask> task() {
    return Task.class;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    var topics = config.list(SourceConnector.TOPICS_KEY, ",");
    if (topics.size() <= maxTasks)
      return topics.stream()
          .map(
              t -> {
                var copy = new HashMap<>(config.raw());
                copy.put(SourceConnector.TOPICS_KEY, t);
                return Configuration.of(copy);
              })
          .collect(Collectors.toUnmodifiableList());
    return Utils.chunk(topics, maxTasks).stream()
        .map(
            tps -> {
              var copy = new HashMap<>(config.raw());
              copy.put(SourceConnector.TOPICS_KEY, String.join(",", tps));
              return Configuration.of(copy);
            })
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(
        THROUGHPUT_DEF,
        KEY_SIZE_DEF,
        KEY_DISTRIBUTION_DEF,
        KEY_SIZE_DISTRIBUTION_DEF,
        VALUE_SIZE_DEF,
        VALUE_DISTRIBUTION_DEF,
        VALUE_SIZE_DISTRIBUTION_DEF,
        BATCH_SIZE_DEF,
        KEY_TABLE_SEED,
        VALUE_TABLE_SEED);
  }

  public static class Task extends SourceTask {
    Set<TopicPartition> specifyPartitions = Set.of();

    RecordGenerator recordGenerator = null;

    @Override
    protected void init(Configuration configuration, MetadataStorage storage) {
      var throughput =
          DataSize.of(
              configuration
                  .string(THROUGHPUT_DEF.name())
                  .orElse(THROUGHPUT_DEF.defaultValue().toString()));
      var KeySize =
          DataSize.of(
              configuration
                  .string(KEY_SIZE_DEF.name())
                  .orElse(KEY_SIZE_DEF.defaultValue().toString()));
      var keyDistribution =
          DistributionType.ofAlias(
              configuration
                  .string(KEY_DISTRIBUTION_DEF.name())
                  .orElse(KEY_DISTRIBUTION_DEF.defaultValue().toString()));
      var keySizeDistribution =
          DistributionType.ofAlias(
              configuration
                  .string(KEY_SIZE_DISTRIBUTION_DEF.name())
                  .orElse(KEY_SIZE_DISTRIBUTION_DEF.defaultValue().toString()));
      var valueSize =
          DataSize.of(
              configuration
                  .string(VALUE_SIZE_DEF.name())
                  .orElse(VALUE_SIZE_DEF.defaultValue().toString()));
      var valueDistribution =
          DistributionType.ofAlias(
              configuration
                  .string(VALUE_DISTRIBUTION_DEF.name())
                  .orElse(VALUE_DISTRIBUTION_DEF.defaultValue().toString()));
      var valueSizeDistribution =
          DistributionType.ofAlias(
              configuration
                  .string(VALUE_SIZE_DISTRIBUTION_DEF.name())
                  .orElse(VALUE_SIZE_DISTRIBUTION_DEF.defaultValue().toString()));

      var batchSize =
          configuration
              .integer(BATCH_SIZE_DEF.name())
              .orElse((Integer) BATCH_SIZE_DEF.defaultValue().get());
      var keyTableSeed =
          configuration
              .longInteger(KEY_TABLE_SEED.name())
              .orElse((Long) KEY_TABLE_SEED.defaultValue().get());
      var valueTableSeed =
          configuration
              .longInteger(VALUE_TABLE_SEED.name())
              .orElse((Long) VALUE_TABLE_SEED.defaultValue().get());

      specifyPartitions =
          configuration.list(SourceConnector.TOPICS_KEY, ",").stream()
              .map(t -> TopicPartition.of(t, -1))
              .collect(Collectors.toUnmodifiableSet());
      recordGenerator =
          RecordGenerator.builder()
              .batchSize(batchSize)
              .keyTableSeed(keyTableSeed)
              .keyRange(
                  LongStream.rangeClosed(0, 10000).boxed().collect(Collectors.toUnmodifiableList()))
              .keyDistribution(keyDistribution.create(10000, configuration))
              .keySizeDistribution(keySizeDistribution.create((int) KeySize.bytes(), configuration))
              .valueTableSeed(valueTableSeed)
              .valueRange(
                  LongStream.rangeClosed(0, 10000).boxed().collect(Collectors.toUnmodifiableList()))
              .valueDistribution(valueDistribution.create(10000, configuration))
              .valueSizeDistribution(
                  valueSizeDistribution.create((int) valueSize.bytes(), configuration))
              .throughput(tp -> throughput.dataRate(Duration.ofSeconds(1)))
              .build();
    }

    @Override
    protected Collection<SourceRecord> take() {
      return specifyPartitions.stream()
          .flatMap(tp -> recordGenerator.apply(tp).stream())
          .map(r -> SourceRecord.builder().record(r).build())
          .toList();
    }
  }
}
