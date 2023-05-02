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

import java.util.concurrent.atomic.DoubleAdder;

/** By contrast to {@link Avg}, {@link Rate} measure the value by time instead of "count". */
public interface Rate<T> extends Stat<T> {

  /**
   * @return sum of recorded value / (current time - start time). Noted that the unit is second.
   */
  static Rate<Double> of() {
    final long start = System.currentTimeMillis();
    var adder = new DoubleAdder();
    return new Rate<>() {
      @Override
      public void record(Double value) {
        adder.add(value);
      }

      @Override
      public Double measure() {
        var diff = System.currentTimeMillis() - start;
        if (diff <= 0) return 0.0;
        return adder.sum() / diff * 1000;
      }
    };
  }
}
