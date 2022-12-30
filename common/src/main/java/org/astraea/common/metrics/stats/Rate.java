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
import org.astraea.common.DataSize;

/** By contrast to {@link Avg}, {@link Rate} measure the value by time instead of "count". */
public interface Rate<T> extends Stat<T> {
  /**
   * @return sum of recorded size / (time of last record - start time). Noted that the unit is
   *     second
   */
  static Rate<DataSize> sizeRate() {
    return new Rate<>() {
      private final long start = System.currentTimeMillis();
      private DataSize size = DataSize.Byte.of(0);

      @Override
      public synchronized void record(DataSize value) {
        size = size.add(value);
      }

      @Override
      public synchronized DataSize measure() {
        var diff = System.currentTimeMillis() - start;
        if (diff <= 0) return DataSize.Byte.of(0);
        return size.dataRate(Duration.ofMillis(diff)).dataSize();
      }
    };
  }
}
