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
import java.util.LinkedList;

/** Ratio of increased/decreased value and past value. */
public class WindowedRate implements Stat<Double, Double> {
  private Double currentValue;

  private final LinkedList<ValueAndTime> past = new LinkedList<>();
  private final long interval;

  public WindowedRate(Duration interval) {
    this(0.0, interval);
  }

  public WindowedRate(double initValue, Duration interval) {
    this.currentValue = initValue;
    this.interval = interval.toMillis();
  }

  @Override
  public synchronized void record(Double value) {
    long current = System.currentTimeMillis();

    // The "currentValue" had not changed until now.
    past.add(new ValueAndTime(currentValue, current));
    this.currentValue += value;
    past.add(new ValueAndTime(value, current));

    removeOutdated();
  }

  // Get the ratio of difference between past value.
  @Override
  public synchronized Double measure() {
    removeOutdated();
    if (past.isEmpty()) return 0.0;
    else return (currentValue - past.peek().value) / past.peek().value;
  }

  // Remove outdated value.
  private void removeOutdated() {
    long current = System.currentTimeMillis();
    while (!past.isEmpty() && past.peek().timestamp < current - this.interval) {
      past.poll();
    }
  }

  private static class ValueAndTime {
    final Double value;
    final long timestamp;

    ValueAndTime(Double value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }
  }
}
