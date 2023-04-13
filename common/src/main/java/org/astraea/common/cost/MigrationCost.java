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
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;

public class MigrationCost {

  public final String name;

  public final Map<Integer, Long> brokerCosts;
  public static final String TO_SYNC_BYTES = "record size to sync (bytes)";
  public static final String TO_FETCH_BYTES = "record size to fetch (bytes)";
  public static final String CHANGED_REPLICAS = "changed replicas";
  public static final String CHANGED_LEADERS = "changed leaders";

  public static List<MigrationCost> migrationCosts(ClusterInfo before, ClusterInfo after) {
    var migrateInBytes = recordSizeToSync(before, after);
    var migrateOutBytes = recordSizeToFetch(before, after);
    var migrateReplicaNum = replicaNumChanged(before, after);
    var migrateReplicaLeader = replicaLeaderChanged(before, after);
    return List.of(
        new MigrationCost(TO_SYNC_BYTES, migrateInBytes),
        new MigrationCost(TO_FETCH_BYTES, migrateOutBytes),
        new MigrationCost(CHANGED_REPLICAS, migrateReplicaNum),
        new MigrationCost(CHANGED_LEADERS, migrateReplicaLeader));
  }

  public MigrationCost(String name, Map<Integer, Long> brokerCosts) {
    this.name = name;
    this.brokerCosts = brokerCosts;
  }

  static Map<Integer, Long> recordSizeToFetch(ClusterInfo before, ClusterInfo after) {
    return changedRecordSize(before, after, true);
  }

  static Map<Integer, Long> recordSizeToSync(ClusterInfo before, ClusterInfo after) {
    return changedRecordSize(before, after, false);
  }

  static Map<Integer, Long> replicaNumChanged(ClusterInfo before, ClusterInfo after) {
    return changedReplicaNumber(before, after, ignore -> true);
  }

  static Map<Integer, Long> replicaLeaderChanged(ClusterInfo before, ClusterInfo after) {
    return changedReplicaNumber(before, after, Replica::isLeader);
  }

  /**
   * @param before the ClusterInfo before migrated replicas
   * @param after the ClusterInfo after migrated replicas
   * @param migrateOut if data log need fetch from replica leader, set this true
   * @return the data size to migrated by all brokers
   */
  private static Map<Integer, Long> changedRecordSize(
      ClusterInfo before, ClusterInfo after, boolean migrateOut) {
    var source = migrateOut ? after : before;
    var dest = migrateOut ? before : after;
    var changePartitions = ClusterInfo.findNonFulfilledAllocation(source, dest);
    var cost =
        changePartitions.stream()
            .flatMap(p -> dest.replicas(p).stream().filter(r -> !source.replicas(p).contains(r)))
            .map(
                r -> {
                  if (migrateOut) return dest.replicaLeader(r.topicPartition()).orElse(r);
                  return r;
                })
            .collect(
                Collectors.groupingBy(
                    r -> r.nodeInfo().id(),
                    Collectors.mapping(
                        Function.identity(), Collectors.summingLong(Replica::size))));
    return Stream.concat(dest.nodes().stream(), source.nodes().stream())
        .map(NodeInfo::id)
        .distinct()
        .parallel()
        .collect(Collectors.toMap(Function.identity(), n -> cost.getOrDefault(n, 0L)));
  }

  static boolean changedRecordSizeOverflow(
      ClusterInfo before, ClusterInfo after, Predicate<Replica> predicate, long limit) {
    var totalRemovedSize = 0L;
    var totalAddedSize = 0L;
    for (var id :
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .parallel()
            .collect(Collectors.toSet())) {
      var removed =
          (int)
              before
                  .replicaStream(id)
                  .filter(predicate)
                  .filter(r -> !after.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
                  .sum();
      var added =
          (int)
              after
                  .replicaStream(id)
                  .filter(predicate)
                  .filter(r -> !before.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
                  .sum();
      totalRemovedSize = totalRemovedSize + removed;
      totalAddedSize = totalAddedSize + added;
      // if migrate cost overflow, leave early and return true
      if (totalRemovedSize > limit || totalAddedSize > limit) return true;
    }
    return Math.max(totalRemovedSize, totalAddedSize) > limit;
  }

  private static Map<Integer, Long> changedReplicaNumber(
      ClusterInfo before, ClusterInfo after, Predicate<Replica> predicate) {
    return Stream.concat(before.nodes().stream(), after.nodes().stream())
        .map(NodeInfo::id)
        .distinct()
        .parallel()
        .collect(
            Collectors.toUnmodifiableMap(
                Function.identity(),
                id -> {
                  var removedLeaders =
                      before
                          .replicaStream(id)
                          .filter(predicate)
                          .filter(
                              r ->
                                  after
                                      .replicaStream(r.topicPartitionReplica())
                                      .noneMatch(predicate))
                          .count();
                  var newLeaders =
                      after
                          .replicaStream(id)
                          .filter(predicate)
                          .filter(
                              r ->
                                  before
                                      .replicaStream(r.topicPartitionReplica())
                                      .noneMatch(predicate))
                          .count();
                  return newLeaders - removedLeaders;
                }));
  }
}
