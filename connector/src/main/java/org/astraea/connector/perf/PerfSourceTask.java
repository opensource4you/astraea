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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.DistributionType;
import org.astraea.common.Utils;
import org.astraea.common.producer.Record;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceTask;

public class PerfSourceTask extends SourceTask {

  final Random rand = new Random();
  Set<String> topics = Set.of();
  Duration frequency;
  Supplier<Long> keySelector;
  Supplier<Long> keySizeGenerator;
  final Map<Long, byte[]> keys = new HashMap<>();
  Supplier<Long> valueSelector;
  Supplier<Long> valueSizeGenerator;
  final Map<Long, byte[]> values = new HashMap<>();
  long last = System.currentTimeMillis();

  @Override
  protected void init(Configuration configuration) {
    this.topics = Set.copyOf(configuration.list(SourceConnector.TOPICS_KEY, ","));
    this.frequency =
        Utils.toDuration(
            configuration
                .string(PerfSource.FREQUENCY_DEF.name())
                .orElse(PerfSource.FREQUENCY_DEF.defaultValue().toString()));
    var keyLength =
        DataSize.of(
            configuration
                .string(PerfSource.KEY_LENGTH_DEF.name())
                .orElse(PerfSource.KEY_LENGTH_DEF.defaultValue().toString()));
    var valueLength =
        DataSize.of(
            configuration
                .string(PerfSource.VALUE_LENGTH_DEF.name())
                .orElse(PerfSource.VALUE_LENGTH_DEF.defaultValue().toString()));
    var keyDistribution =
        DistributionType.ofAlias(
            configuration
                .string(PerfSource.KEY_DISTRIBUTION_DEF.name())
                .orElse(PerfSource.KEY_DISTRIBUTION_DEF.defaultValue().toString()));
    var valueDistribution =
        DistributionType.ofAlias(
            configuration
                .string(PerfSource.VALUE_DISTRIBUTION_DEF.name())
                .orElse(PerfSource.VALUE_DISTRIBUTION_DEF.defaultValue().toString()));
    keySelector = keyDistribution.create(10000);
    keySizeGenerator = keyDistribution.create(keyLength.measurement(DataUnit.Byte).intValue());
    valueSelector = valueDistribution.create(10000);
    valueSizeGenerator =
        valueDistribution.create(valueLength.measurement(DataUnit.Byte).intValue());
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
      return topics.stream()
          .map(t -> Record.builder().topic(t).key(key()).value(value()).build())
          .collect(Collectors.toList());
    } finally {
      last = System.currentTimeMillis();
    }
  }
}
