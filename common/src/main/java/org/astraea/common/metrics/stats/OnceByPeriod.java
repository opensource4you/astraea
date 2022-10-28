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

/** Return a value */
public class OnceByPeriod<V> implements RangedDataCalculator<V> {
  private final long periodMillis;
  private long lastTimestamp = -1;

  public OnceByPeriod(Duration period) {
    this.periodMillis = period.toMillis();
  }

  @Override
  public Optional<V> record(V value, long timestamp) {
    if (lastTimestamp != -1 && timestamp < lastTimestamp + periodMillis) {
      return Optional.empty();
    }
    lastTimestamp = timestamp;
    return Optional.of(value);
  }
}
