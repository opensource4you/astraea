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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** By contrast to {@link Avg}, {@link Rate} measure the value by time instead of "count". */
public interface Rate extends Stat<Long> {
  /**
   * @param unit of rate
   * @return sum of recorded values / (time of last record - start time)
   */
  static Rate of(TimeUnit unit) {
    return new Rate() {
      private final long start = System.currentTimeMillis();
      private volatile long last = System.currentTimeMillis();
      private final AtomicLong sum = new AtomicLong();

      @Override
      public void record(Long value) {
        sum.addAndGet(value);
        last = System.currentTimeMillis();
      }

      @Override
      public Long measure() {
        var diff = last - start;
        if (diff <= 0) return 0L;
        return (long) (sum.doubleValue() / unit.convert(diff, TimeUnit.MILLISECONDS));
      }
    };
  }
}
