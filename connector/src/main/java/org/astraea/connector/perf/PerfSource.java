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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigException;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.DistributionType;
import org.astraea.common.Utils;
import org.astraea.common.producer.Record;
import org.astraea.connector.Definition;
import org.astraea.connector.MetadataStorage;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceTask;

public class PerfSource extends SourceConnector {
  static Definition FREQUENCY_DEF =
      Definition.builder()
          .name("frequency.in.seconds")
          .type(Definition.Type.STRING)
          .defaultValue("1s")
          .validator((name, value) -> Utils.toDuration(value.toString()))
          .build();
  static Definition KEY_LENGTH_DEF =
      Definition.builder()
          .name("key.length")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue(DataSize.Byte.of(50).toString())
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
  static Definition VALUE_LENGTH_DEF =
      Definition.builder()
          .name("value.length")
          .type(Definition.Type.STRING)
          .validator((name, obj) -> DataSize.of(obj.toString()))
          .defaultValue(DataSize.KB.of(1).toString())
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

  static Definition SPECIFY_PARTITIONS_DEF =
      Definition.builder()
          .name("specify.partitions")
          .type(Definition.Type.STRING)
          .validator(
              (name, obj) -> {
                if (obj == null) return;
                if (obj instanceof String) {
                  Arrays.stream(((String) obj).split(",")).forEach(Integer::parseInt);
                  return;
                }
                throw new ConfigException(name, obj, "there are non-number strings");
              })
          .documentation(
              "If this config is defined, all records will be sent to those given partitions")
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
    return IntStream.range(0, maxTasks).mapToObj(i -> config).collect(Collectors.toList());
  }

  @Override
  protected List<Definition> definitions() {
    return List.of(
        FREQUENCY_DEF,
        KEY_LENGTH_DEF,
        KEY_DISTRIBUTION_DEF,
        VALUE_LENGTH_DEF,
        VALUE_DISTRIBUTION_DEF,
        SPECIFY_PARTITIONS_DEF);
  }

  public static class Task extends SourceTask {

    final Random rand = new Random();
    Set<String> topics = Set.of();
    Duration frequency;
    Supplier<Long> keySelector;
    Supplier<Long> keySizeGenerator;
    final Map<Long, byte[]> keys = new HashMap<>();
    Supplier<Long> valueSelector;
    Supplier<Long> valueSizeGenerator;
    final Map<Long, byte[]> values = new HashMap<>();

    List<Integer> specifyPartitions = List.of();

    long last = System.currentTimeMillis();

    @Override
    protected void init(Configuration configuration, MetadataStorage storage) {
      this.topics = Set.copyOf(configuration.list(SourceConnector.TOPICS_KEY, ","));
      this.frequency =
          Utils.toDuration(
              configuration
                  .string(FREQUENCY_DEF.name())
                  .orElse(FREQUENCY_DEF.defaultValue().toString()));
      var keyLength =
          DataSize.of(
              configuration
                  .string(KEY_LENGTH_DEF.name())
                  .orElse(KEY_LENGTH_DEF.defaultValue().toString()));
      var valueLength =
          DataSize.of(
              configuration
                  .string(VALUE_LENGTH_DEF.name())
                  .orElse(VALUE_LENGTH_DEF.defaultValue().toString()));
      var keyDistribution =
          DistributionType.ofAlias(
              configuration
                  .string(KEY_DISTRIBUTION_DEF.name())
                  .orElse(KEY_DISTRIBUTION_DEF.defaultValue().toString()));
      var valueDistribution =
          DistributionType.ofAlias(
              configuration
                  .string(VALUE_DISTRIBUTION_DEF.name())
                  .orElse(VALUE_DISTRIBUTION_DEF.defaultValue().toString()));
      keySelector = keyDistribution.create(10000);
      keySizeGenerator = keyDistribution.create(keyLength.measurement(DataUnit.Byte).intValue());
      valueSelector = valueDistribution.create(10000);
      valueSizeGenerator =
          valueDistribution.create(valueLength.measurement(DataUnit.Byte).intValue());
      specifyPartitions =
          configuration
              .string(SPECIFY_PARTITIONS_DEF.name())
              .map(
                  s ->
                      Arrays.stream(s.split(","))
                          .map(Integer::parseInt)
                          .collect(Collectors.toList()))
              .orElse(List.of());
    }

    byte[] key() {
      var size = keySizeGenerator.get().intValue();
      // user can define zero size for key
      if (size == 0) return null;
      return keys.computeIfAbsent(
          keySelector.get(),
          ignored -> {
            var value = new byte[size];
            rand.nextBytes(value);
            return value;
          });
    }

    byte[] value() {
      var size = valueSizeGenerator.get().intValue();
      // user can define zero size for value
      if (size == 0) return null;
      return values.computeIfAbsent(
          valueSelector.get(),
          ignored -> {
            var value = new byte[size];
            rand.nextBytes(value);
            return value;
          });
    }

    @Override
    protected Collection<Record<byte[], byte[]>> take() {
      if (System.currentTimeMillis() - last < frequency.toMillis()) return List.of();
      try {
        if (specifyPartitions.isEmpty())
          return topics.stream()
              .map(t -> Record.builder().topic(t).key(key()).value(value()).build())
              .collect(Collectors.toList());
        return topics.stream()
            .flatMap(
                t ->
                    specifyPartitions.stream()
                        .map(
                            p ->
                                Record.builder()
                                    .topic(t)
                                    .partition(p)
                                    .key(key())
                                    .value(value())
                                    .build()))
            .collect(Collectors.toList());
      } finally {
        last = System.currentTimeMillis();
      }
    }
  }
}
