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

/**
 * Ratio of increased/decreased value and past value. This statistic records the all the values and
 * clear the out-dated values at the same time. Every time {@link #measure()} is called, it will
 * clear the out-dated values and return the increasing rate. The increasing rate is calculated by
 * (latest-oldest)/oldest. As a result, the returned value may be negative. Depend on the value
 * increasing or decreasing. For example, if we have a windowed-rate with time interval 2.
 *
 * <p><figure>
 *
 * <table>
 * <thead><tr><th>Time</th><th>Record Value</th><th>Current Value</th><th>measurement</th></tr></thead>
 * <tbody>
 *     <tr><td>0</td><td>+10</td><td>10</td><td>0.0</td></tr>
 *     <tr><td>1</td><td>+5</td><td>15</td><td>(15-10)/10 = 0.5</td></tr>
 *     <tr><td>2</td><td>&nbsp;</td><td>15</td><td>(15-10)/10 = 0.5</td></tr>
 *     <tr><td>3</td><td>-5</td><td>10</td><td>(10-15)/15 = -0.33</td></tr>
 *     <tr><td>4</td><td>&nbsp;</td><td>10</td><td>(10-15)/15 = -0.33</td></tr>
 *     <tr><td>5</td><td>&nbsp;</td><td>10</td><td>0.0</td></tr>
 * </tbody>
 * </table>
 *
 * </figure>
 *
 * <p>At time 0, we record the value +10. At the same time, if we measure this windowed-rate, we
 * will get 0.0. Because there is no past value to compute the ratio. At time 1, we record the value
 * +5. Now we can compute the ratio. As a result, we get 0.5 when we {@link #measure()} this stat.
 * At time 2, the {@link #measure()} is still 0.5. At time 3, we record the value -5. Then we will
 * measure -0.33. A negative ratio means the value decrease. At time 4, it is the same as time 3. At
 * time 5, the out-dated value is cleared. The measurement becomes 0.0. Meaning that the value do
 * not change in the time interval.
 */
public class WindowedRate implements Stat<Double> {
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
  private synchronized void removeOutdated() {
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
