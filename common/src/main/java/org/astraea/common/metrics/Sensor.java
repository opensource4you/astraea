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
package org.astraea.common.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.metrics.stats.Stat;

public interface Sensor<V> {

  static Builder<Long> builder() {
    return new Builder<>();
  }

  /** Record the new get data */
  void record(V value);

  /**
   * Get the statistic by the given `metricName`.
   *
   * @param statName key to get the measurement
   * @return the value calculated by the corresponding `Stat`
   */
  V measure(String statName);

  Map<String, Stat<V>> metrics();

  class Builder<V> {
    private final Map<String, Stat<?>> stats = new HashMap<>();

    public Builder() {}

    @SuppressWarnings("unchecked")
    public <NewV> Builder<NewV> addStat(String name, Stat<NewV> stat) {
      stats.put(name, stat);
      return (Builder<NewV>) this;
    }

    @SuppressWarnings("unchecked")
    public <NewV> Builder<NewV> addStats(Map<String, Stat<NewV>> stats) {
      this.stats.putAll(stats);
      return (Builder<NewV>) this;
    }

    public Sensor<V> build() {
      @SuppressWarnings("unchecked")
      var stats =
          this.stats.entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> (Stat<V>) e.getValue()));
      return new Sensor<>() {
        @Override
        public synchronized void record(V value) {
          stats.values().forEach(stat -> stat.record(value));
        }

        @Override
        public V measure(String statName) {
          return stats.get(statName).measure();
        }

        @Override
        public synchronized Map<String, Stat<V>> metrics() {
          return stats.entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
        }
      };
    }
  }
}
