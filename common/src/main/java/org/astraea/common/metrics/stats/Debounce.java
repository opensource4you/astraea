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
package org.astraea.common.metrics.stats;

import java.time.Duration;
import java.util.Optional;

/**
 * Not all data should be used in statistic. Gauge like "topic size" is a time related data, many
 * records recorded at the same time should be considered as one record.
 */
public interface Debounce<V> {
  Optional<V> record(V value, long timestamp);

  static <V> Debounce<V> of(Duration duration) {
    return new Debounce<>() {
      private long lastTimestamp = -1;

      @Override
      public Optional<V> record(V value, long timestamp) {
        if (lastTimestamp != -1 && timestamp < lastTimestamp + duration.toMillis()) {
          return Optional.empty();
        }
        lastTimestamp = timestamp;
        return Optional.of(value);
      }
    };
  }
}
