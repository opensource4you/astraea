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
package org.astraea.common.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.DistributionType;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.stats.Rate;

@FunctionalInterface
public interface RecordGenerator extends Function<TopicPartition, List<Record<byte[], byte[]>>> {
  static Builder builder() {
    return new Builder();
  }

  class Builder {

    int batchSize = 1;
    long keyTableSeed = ThreadLocalRandom.current().nextLong();
    long valueTableSeed = ThreadLocalRandom.current().nextLong();
    List<Long> keyRange =
        LongStream.rangeClosed(0, 10000).boxed().collect(Collectors.toUnmodifiableList());
    Supplier<Long> keyDistribution = DistributionType.UNIFORM.create(10000, Configuration.EMPTY);
    Supplier<Long> keySizeDistribution =
        DistributionType.UNIFORM.create(10000, Configuration.EMPTY);
    List<Long> valueRange =
        LongStream.rangeClosed(0, 10000).boxed().collect(Collectors.toUnmodifiableList());
    Supplier<Long> valueDistribution = DistributionType.UNIFORM.create(10000, Configuration.EMPTY);
    Supplier<Long> valueSizeDistribution =
        DistributionType.UNIFORM.create(10000, Configuration.EMPTY);
    Function<TopicPartition, DataRate> throughput = ignored -> null;

    private Builder() {}

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder keyTableSeed(long keyTableSeed) {
      this.keyTableSeed = keyTableSeed;
      return this;
    }

    public Builder valueTableSeed(long valueTableSeed) {
      this.valueTableSeed = valueTableSeed;
      return this;
    }

    public Builder keyRange(List<Long> keyRange) {
      this.keyRange = keyRange;
      return this;
    }

    public Builder keyDistribution(Supplier<Long> keyDistribution) {
      this.keyDistribution = keyDistribution;
      return this;
    }

    public Builder keySizeDistribution(Supplier<Long> keySizeDistribution) {
      this.keySizeDistribution = keySizeDistribution;
      return this;
    }

    public Builder valueRange(List<Long> valueRange) {
      this.valueRange = valueRange;
      return this;
    }

    public Builder valueDistribution(Supplier<Long> valueDistribution) {
      this.valueDistribution = valueDistribution;
      return this;
    }

    public Builder valueSizeDistribution(Supplier<Long> valueSizeDistribution) {
      this.valueSizeDistribution = valueSizeDistribution;
      return this;
    }

    public Builder throughput(Function<TopicPartition, DataRate> throughput) {
      this.throughput = throughput;
      return this;
    }

    public RecordGenerator build() {
      final var keyRandom = new Random(keyTableSeed);
      final var valueRandom = new Random(valueTableSeed);
      final Map<Long, byte[]> recordKeyTable =
          keyRange.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      index -> index,
                      index -> {
                        var size = keySizeDistribution.get().intValue();
                        var key = new byte[size];
                        keyRandom.nextBytes(key);
                        return key;
                      }));
      final Map<Long, byte[]> recordValueTable =
          valueRange.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      index -> index,
                      index -> {
                        var size = valueSizeDistribution.get().intValue();
                        var value = new byte[size];
                        valueRandom.nextBytes(value);
                        return value;
                      }));

      Supplier<byte[]> keySupplier =
          () -> {
            var key = recordKeyTable.get(keyDistribution.get());
            // Intentionally replace key with zero length by null key. A key with zero length can
            // lead to ambiguous behavior in an experiment.
            // See https://github.com/skiptests/astraea/pull/1521#discussion_r1121801293 for further
            // details.
            return key != null && key.length > 0 ? key : null;
          };
      Supplier<byte[]> valueSupplier =
          () -> {
            var value = recordValueTable.get(valueDistribution.get());
            // same behavior as key
            return value != null && value.length > 0 ? value : null;
          };

      var throttlers = new HashMap<TopicPartition, Function<Long, Boolean>>();
      return (tp) -> {
        var throttler =
            throttlers.computeIfAbsent(
                tp,
                ignored ->
                    Optional.ofNullable(throughput.apply(tp))
                        .map(RecordGenerator::throttler)
                        .orElse(size -> false));
        var records = new ArrayList<Record<byte[], byte[]>>(batchSize);
        for (int i = 0; i < batchSize; i++) {
          var key = keySupplier.get();
          var value = valueSupplier.get();
          long size = (value != null ? value.length : 0) + (key != null ? key.length : 0);
          if (throttler.apply(size)) return List.of();
          records.add(
              Record.builder()
                  .key(key)
                  .value(value)
                  .topicPartition(tp)
                  .timestamp(System.currentTimeMillis())
                  .build());
        }
        return records;
      };
    }
  }

  static Function<Long, Boolean> throttler(DataRate max) {
    final var throughput = max.byteRate();
    final var rate = Rate.of();
    return payloadLength -> {
      if (rate.measure() >= throughput) return true;
      rate.record((double) payloadLength);
      return false;
    };
  }
}
