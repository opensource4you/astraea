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
package org.astraea.common.cost;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.collector.Fetcher;

/** more replicas migrate -> higher cost */
public class ReplicaNumberCost implements HasClusterCost, HasMoveCost {
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.empty();
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    return MoveCost.changedReplicaCount(
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .distinct()
            .parallel()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    id -> {
                      var removedReplicas =
                          (int)
                              before
                                  .replicaStream(id)
                                  .filter(
                                      r ->
                                          after.replicaStream(r.topicPartitionReplica()).count()
                                              == 0)
                                  .count();
                      var newReplicas =
                          (int)
                              after
                                  .replicaStream(id)
                                  .filter(
                                      r ->
                                          before.replicaStream(r.topicPartitionReplica()).count()
                                              == 0)
                                  .count();
                      return newReplicas - removedReplicas;
                    })));
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    // there is no better plan for single node
    if (clusterInfo.nodes().size() == 1) return () -> 0;

    var group =
        clusterInfo.replicas().stream().collect(Collectors.groupingBy(r -> r.nodeInfo().id()));

    // worst case: all partitions are hosted by single node
    if (clusterInfo.nodes().size() > 1 && group.size() <= 1) return () -> Long.MAX_VALUE;

    // there is a node having zero replica!
    if (clusterInfo.nodes().stream().anyMatch(node -> !group.containsKey(node.id())))
      return () -> Long.MAX_VALUE;

    // normal case
    var max = group.values().stream().mapToLong(List::size).max().orElse(0);
    var min = group.values().stream().mapToLong(List::size).min().orElse(0);
    return () -> max - min;
  }
}
