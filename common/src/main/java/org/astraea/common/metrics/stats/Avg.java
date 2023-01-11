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
import java.util.concurrent.ConcurrentLinkedDeque;

public class Avg {
  public static Stat<Double> of() {
    return new Stat<>() {
      private int counter = 0;
      private double accumulator = 0.0;

      @Override
      public synchronized void record(Double value) {
        ++counter;
        accumulator += value;
      }

      @Override
      public synchronized Double measure() {
        if (counter == 0) return Double.NaN;
        return accumulator / counter;
      }
    };
  }

  public static Stat<Double> expWeightByTime(Duration period) {
    return expWeightByTime(period, 0.5);
  }

  /**
   * This class implements {@link Stat} using an exponential moving average, an exponentially
   * decreasing weighted moving average that weights past data based on a given alpha value and adds
   * up to get the average. When new data comes in, the calculation method is as follows: Average =
   * new data * alpha + past data * (1-alpha) , the default value of alpha is 0.5.
   *
   * @param period Set the interval time for obtaining indicators. If multiple values are obtained
   *     within the duration, it will be regarded as one.
   * @param alpha alpha indicates how much you value new data. The larger the value, the lower the
   *     weight of the past data, the weight of the latest data is alpha, the weight of the past
   *     data is 1-alpha, the alpha needs to be between 0 and 1.
   */
  public static Stat<Double> expWeightByTime(Duration period, double alpha) {
    return new Stat<>() {
      private double accumulate = 0.0;
      private final Debounce<Double> debounce = Debounce.of(period);

      @Override
      public synchronized void record(Double value) {
        long current = System.currentTimeMillis();
        debounce
            .record(value, current)
            .ifPresent(
                debouncedValue -> accumulate = accumulate * (1 - alpha) + debouncedValue * alpha);
      }

      @Override
      public synchronized Double measure() {
        return accumulate;
      }
    };
  }

  /** Compute the average of value recorded in the given time period. */
  public static Stat<Double> byTime(Duration period) {
    if (period.toMillis() <= 0) {
      throw new IllegalArgumentException(
          "Stat, Average by time, needs period longer than 1 millisecond.");
    }
    return new Stat<>() {
      private final ConcurrentLinkedDeque<ValueAndTime<Double>> past =
          new ConcurrentLinkedDeque<>();

      @Override
      public void record(Double value) {
        past.add(new ValueAndTime<>(value, System.currentTimeMillis()));
        popOutdated();
      }

      @Override
      public Double measure() {
        popOutdated();
        return past.stream().mapToDouble(e -> e.value).average().orElse(Double.NaN);
      }

      private void popOutdated() {
        var outdated = System.currentTimeMillis() - period.toMillis();
        while (!past.isEmpty() && past.peekFirst().timestamp < outdated) {
          past.poll();
        }
      }
    };
  }

  private static class ValueAndTime<V> {
    public final V value;
    public final long timestamp;

    public ValueAndTime(V value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }
  }
}
