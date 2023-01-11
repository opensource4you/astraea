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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Lazy;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;

public class RoundRobinKeeper {
  private final AtomicInteger next = new AtomicInteger(0);
  final int[] roundRobin;
  final Duration roundRobinLease;
  volatile long timeToUpdateRoundRobin = -1;

  private RoundRobinKeeper(int preLength, Duration roundRobinLease) {
    this.roundRobin = new int[preLength];
    this.roundRobinLease = roundRobinLease;
  }

  static RoundRobinKeeper of(int preLength, Duration roundRobinLease) {
    return new RoundRobinKeeper(preLength, roundRobinLease);
  }

  synchronized void tryToUpdate(ClusterInfo clusterInfo, Lazy<Map<Integer, Double>> costToScore) {
    if (System.currentTimeMillis() >= timeToUpdateRoundRobin) {
      var roundRobin = RoundRobin.smooth(costToScore.get());
      var ids =
          clusterInfo.nodes().stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
      // TODO: make ROUND_ROBIN_LENGTH configurable ???
      IntStream.range(0, this.roundRobin.length)
          .forEach(index -> this.roundRobin[index] = roundRobin.next(ids).orElse(-1));
      timeToUpdateRoundRobin = System.currentTimeMillis() + roundRobinLease.toMillis();
    }
  }

  int next() {
    return roundRobin[
        next.getAndUpdate(previous -> previous >= roundRobin.length - 1 ? 0 : previous + 1)];
  }
}
