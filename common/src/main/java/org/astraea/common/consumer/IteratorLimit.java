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
package org.astraea.common.consumer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@FunctionalInterface
public interface IteratorLimit<Key, Value> {

  /** limit the max size of all fetched records */
  static <Key, Value> IteratorLimit<Key, Value> size(long size) {
    var sum = new AtomicLong();
    return current ->
        sum.addAndGet(
                current.stream()
                    .mapToLong(r -> r.serializedKeySize() + r.serializedValueSize())
                    .sum())
            >= size;
  }

  /** limit the max number of all fetched records */
  static <Key, Value> IteratorLimit<Key, Value> count(int numberOfRecords) {
    var count = new AtomicInteger();
    return current -> count.getAndAdd(current.size()) >= numberOfRecords;
  }

  /** limit the elapsed time of fetching records */
  static <Key, Value> IteratorLimit<Key, Value> elapsed(Duration elapsed) {
    var end = System.currentTimeMillis() + elapsed.toMillis();
    return ignored -> System.currentTimeMillis() >= end;
  }

  /** limit the idle time of fetch records. The "idle" means there is no fetch-able records. */
  static <Key, Value> IteratorLimit<Key, Value> idle(Duration timeout) {
    var lastFetchRecords = new AtomicLong(System.currentTimeMillis());
    return current -> {
      if (!current.isEmpty()) {
        lastFetchRecords.set(System.currentTimeMillis());
        return false;
      }
      // timeout of idle poll is expired, so it is time to break the loop
      return System.currentTimeMillis() - lastFetchRecords.get() >= timeout.toMillis();
    };
  }

  boolean done(List<Record<Key, Value>> current);
}
