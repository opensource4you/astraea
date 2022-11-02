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

/** Calculate the difference of the latest two ranged data. */
public class RateByTime implements Stat<Double, Double> {

  private final Double[] oldValue = new Double[2];

  private final Debounce<Double> debounce;

  public RateByTime(Duration period) {
    this.debounce = Debounce.of(period);
  }

  @Override
  public synchronized void record(Double value) {
    long current = System.currentTimeMillis();
    // Update when a new record occurred
    debounce
        .record(value, current)
        .ifPresent(
            debouncedValue -> {
              oldValue[0] = oldValue[1];
              oldValue[1] = debouncedValue;
            });
  }

  @Override
  public synchronized Double measure() {
    return oldValue[1] - oldValue[0];
  }
}
