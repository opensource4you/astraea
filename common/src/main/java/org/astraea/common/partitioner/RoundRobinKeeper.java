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
package org.astraea.common.partitioner;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;

public class RoundRobinKeeper {
  private final AtomicInteger next = new AtomicInteger(0);
  final int[] roundRobin;
  final Duration roundRobinLease;
  final AtomicLong lastUpdated = new AtomicLong(-1);

  private RoundRobinKeeper(int length, Duration roundRobinLease) {
    this.roundRobin = new int[length];
    this.roundRobinLease = roundRobinLease;
  }

  static RoundRobinKeeper of(int preLength, Duration roundRobinLease) {
    return new RoundRobinKeeper(preLength, roundRobinLease);
  }

  void tryToUpdate(ClusterInfo clusterInfo, Supplier<Map<Integer, Double>> costToScore) {
    var now = System.nanoTime();
    if (lastUpdated.updateAndGet(last -> now - roundRobinLease.toNanos() >= last ? now : last)
        == now) {
      var roundRobin = RoundRobin.smooth(costToScore.get());
      var ids =
          clusterInfo.brokers().stream().map(Broker::id).collect(Collectors.toUnmodifiableSet());
      // TODO: make ROUND_ROBIN_LENGTH configurable ???
      for (var index = 0; index < this.roundRobin.length; ++index)
        this.roundRobin[index] = roundRobin.next(ids).orElse(-1);
    }
  }

  int next() {
    return roundRobin[
        next.getAndUpdate(previous -> previous >= roundRobin.length - 1 ? 0 : previous + 1)];
  }
}
