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

/**
 * This class implements "Stat" using an exponential moving average, an exponentially decreasing
 * weighted moving average that weights past data based on a given alpha value and adds up to get
 * the average. When new data comes in, the calculation method is as follows: Average = new data *
 * alpha + past data * (1-alpha) , the default value of alpha is 0.5.
 */
public class ExpWeightAvgByTime implements Stat<Double> {
  private double accumulate = 0.0;
  private final Debounce<Double> debounce;
  private final double alpha;

  /**
   * @param period Set the interval time for obtaining indicators. If multiple values are obtained
   *     within the duration, it will be regarded as one.
   * @param alpha alpha indicates how much you value new data. The larger the value, the lower the
   *     weight of the past data, the weight of the latest data is alpha, the weight of the past
   *     data is 1-alpha, the alpha needs to be between 0 and 1.
   */
  public ExpWeightAvgByTime(Duration period, double alpha) {
    this.debounce = Debounce.of(period);
    this.alpha = alpha;
  }

  public ExpWeightAvgByTime(Duration period) {
    this.debounce = Debounce.of(period);
    this.alpha = 0.5;
  }

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
}
