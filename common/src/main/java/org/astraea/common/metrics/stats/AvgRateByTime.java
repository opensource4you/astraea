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

public class AvgRateByTime implements Stat<Double, Double> {
  private Double accumulate = 0.0;

  private long count = 0;

  private final Debounce<Double> debounce;

  public AvgRateByTime(Duration period) {
    this.debounce = Debounce.of(period);
  }

  @Override
  public synchronized void record(Double value) {
    long current = System.currentTimeMillis();
    debounce
        .record(value, current)
        .ifPresent(
            debouncedValue -> {
              accumulate += debouncedValue;
              ++count;
            });
  }

  @Override
  public synchronized Double measure() {
    return accumulate / count;
  }
}
