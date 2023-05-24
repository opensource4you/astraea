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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Maintain values in time order. Remove outdated values on new value added or querying.
 *
 * <pre>{@code
 * var windowedValue = new WindowedValue<Integer>(Duration.ofMillis(20));
 * windowedValue.add(1);
 * windowedValue.add(2);
 * Assertions.assertEquals(List.of(1,2), windowedValue.get());
 * }</pre>
 */
class WindowedValue<V> {
  private final ConcurrentLinkedDeque<ValueAndTime<V>> values = new ConcurrentLinkedDeque<>();
  private final Duration window;

  WindowedValue(Duration window) {
    this.window = window;
  }

  void add(V value) {
    values.add(new ValueAndTime<>(value, System.currentTimeMillis()));
    removeAllOutdated();
  }

  List<V> get() {
    removeAllOutdated();
    return values.stream().map(ValueAndTime::value).toList();
  }

  private synchronized void removeAllOutdated() {
    var outdated = System.currentTimeMillis() - window.toMillis();
    while (!values.isEmpty() && values.peekFirst().timestamp < outdated) {
      values.poll();
    }
  }

  private record ValueAndTime<V>(V value, long timestamp) {}
}
