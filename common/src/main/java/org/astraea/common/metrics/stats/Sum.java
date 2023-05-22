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
import java.util.concurrent.atomic.LongAdder;

public interface Sum<T> extends Stat<T> {
  static Sum<Long> ofLong() {
    return new Sum<>() {
      private final LongAdder sum = new LongAdder();

      @Override
      public void record(Long value) {
        sum.add(value);
      }

      @Override
      public Long measure() {
        return sum.sum();
      }
    };
  }

  static Sum<Long> longByTime(Duration window) {
    return new Sum<>() {
      private final WindowedValue<Long> windowedValue = new WindowedValue<>(window);

      @Override
      public void record(Long value) {
        windowedValue.add(value);
      }

      @Override
      public Long measure() {
        return windowedValue.get().stream().mapToLong(l -> l).sum();
      }
    };
  }
}
