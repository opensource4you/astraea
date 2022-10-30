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
package org.astraea.common.balancer.log;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

/**
 * Describe the log allocation state that is associate with a subset of topic/partition of a Kafka
 * cluster.
 */
public interface ClusterLogAllocation {

  static ClusterLogAllocation of(ClusterInfo<Replica> clusterInfo) {
    return of(clusterInfo.replicas());
  }

  /**
   * Construct a {@link ClusterLogAllocation} from the given list of {@link Replica}.
   *
   * <p>Be aware that this class describes <strong>the replica lists of a subset of
   * topic/partitions</strong>. It doesn't require the topic/partition part to have cluster-wide
   * complete information. But the replica list has to be complete. Provide a partial replica list
   * might result in data loss or unintended replica drop during rebalance plan proposing &
   * execution.
   */
  static ClusterLogAllocation of(List<Replica> allocation) {
    return new ClusterLogAllocationImpl(allocation);
  }

  /**
   * let specific broker leave the replica set and let another broker join the replica set.
   *
   * @param replica the replica to perform replica migration
   * @param toBroker the id of the broker about to replace the removed broker
   * @param toDir the absolute path of the data directory this migrated replica is supposed to be on
   *     the destination broker. This value cannot null.
   */
  ClusterLogAllocation migrateReplica(TopicPartitionReplica replica, int toBroker, String toDir);

  /** let specific replica become the preferred leader of its associated topic/partition. */
  ClusterLogAllocation becomeLeader(TopicPartitionReplica replica);

  /**
   * Retrieve the log placements of a specific {@link TopicPartition}.
   *
   * @param topicPartition to query
   * @return log placements or empty collection if there is no log placements
   */
  Set<Replica> logPlacements(TopicPartition topicPartition);

  /** @return all log placements recorded by this {@link ClusterLogAllocation} */
  Set<Replica> logPlacements();

  /** @return all topic/partitions recorded by this {@link ClusterLogAllocation} */
  Set<TopicPartition> topicPartitions();

  /**
   * Find a subset of topic/partitions in the source allocation, that has any non-fulfilled log
   * placement in the given target allocation. Note that the given two allocations must have the
   * exactly same topic/partitions set. Otherwise, an {@link IllegalArgumentException} will be
   * raised.
   */
  static Set<TopicPartition> findNonFulfilledAllocation(
      ClusterLogAllocation source, ClusterLogAllocation target) {

    final var sourceTopicPartition = source.topicPartitions();
    final var targetTopicPartition = target.topicPartitions();
    final var unknownTopicPartitions =
        targetTopicPartition.stream()
            .filter(tp -> !sourceTopicPartition.contains(tp))
            .collect(Collectors.toUnmodifiableSet());

    if (!unknownTopicPartitions.isEmpty())
      throw new IllegalArgumentException(
          "target topic/partition should be a subset of source topic/partition: "
              + unknownTopicPartitions);

    return targetTopicPartition.stream()
        .filter(tp -> !placementMatch(source.logPlacements(tp), target.logPlacements(tp)))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Determine if both of the replicas can be considered as equal in terms of its placement.
   *
   * @param sourceReplicas the source replicas. null value of {@link Replica#path()} will be
   *     interpreted as the actual location doesn't matter.
   * @param targetReplicas the target replicas. null value of {@link Replica#path()} will be
   *     interpreted as the actual location is unknown.
   * @return true if both replicas of specific topic/partitions can be considered as equal in terms
   *     of its placement.
   */
  static boolean placementMatch(Set<Replica> sourceReplicas, Set<Replica> targetReplicas) {
    if (sourceReplicas.size() != targetReplicas.size()) return false;
    final var sourceIds =
        sourceReplicas.stream()
            .sorted(
                Comparator.comparing(Replica::isPreferredLeader)
                    .reversed()
                    .thenComparing(r -> r.nodeInfo().id()))
            .collect(Collectors.toUnmodifiableList());
    final var targetIds =
        targetReplicas.stream()
            .sorted(
                Comparator.comparing(Replica::isPreferredLeader)
                    .reversed()
                    .thenComparing(r -> r.nodeInfo().id()))
            .collect(Collectors.toUnmodifiableList());
    return IntStream.range(0, sourceIds.size())
        .allMatch(
            index -> {
              final var source = sourceIds.get(index);
              final var target = targetIds.get(index);
              return source.isPreferredLeader() == target.isPreferredLeader()
                  && source.nodeInfo().id() == target.nodeInfo().id()
                  && Objects.equals(source.path(), target.path());
            });
  }

  static String toString(ClusterLogAllocation allocation) {
    StringBuilder stringBuilder = new StringBuilder();

    allocation.topicPartitions().stream()
        .sorted()
        .forEach(
            tp -> {
              stringBuilder.append("[").append(tp).append("] ");

              allocation
                  .logPlacements(tp)
                  .forEach(
                      log ->
                          stringBuilder.append(
                              String.format("(%s, %s) ", log.nodeInfo().id(), log.path())));

              stringBuilder.append(System.lineSeparator());
            });

    return stringBuilder.toString();
  }

  class ClusterLogAllocationImpl implements ClusterLogAllocation {

    // maintain this map as an index of tp to replica list to avoid excessive search
    private final Map<TopicPartition, Set<Replica>> allocation;

    private ClusterLogAllocationImpl(List<Replica> allocation) {
      this.allocation =
          allocation.stream()
              .collect(
                  Collectors.groupingBy(
                      ReplicaInfo::topicPartition,
                      Collectors.collectingAndThen(
                          Collectors.toUnmodifiableSet(),
                          (replicas) -> {
                            var hasFuture =
                                replicas.stream()
                                    .filter(Replica::isFuture)
                                    .map(ReplicaInfo::nodeInfo)
                                    .collect(Collectors.toUnmodifiableList());
                            return replicas.stream()
                                .filter(
                                    replica ->
                                        replica.isFuture()
                                            || !hasFuture.contains(replica.nodeInfo()))
                                .collect(Collectors.toUnmodifiableSet());
                          })));

      this.allocation.forEach(
          (topicPartition, replicas) -> {
            // sanity check: no duplicate preferred leader
            var preferredLeaderCount = replicas.stream().filter(Replica::isPreferredLeader).count();
            if (preferredLeaderCount > 1)
              throw new IllegalArgumentException(
                  "Duplicate preferred leader in " + topicPartition + ". " + replicas);
            if (preferredLeaderCount < 1)
              throw new IllegalArgumentException(
                  "Illegal preferred leader count in "
                      + topicPartition
                      + ": "
                      + preferredLeaderCount
                      + ". "
                      + replicas);
            // sanity check: no duplicate node info
            if (replicas.stream().map(ReplicaInfo::nodeInfo).map(NodeInfo::id).distinct().count()
                != replicas.size())
              throw new IllegalArgumentException(
                  "Duplicate replica inside the replica list of "
                      + topicPartition
                      + ". "
                      + replicas);
          });
    }

    @Override
    public ClusterLogAllocation migrateReplica(
        TopicPartitionReplica replica, int toBroker, String toDir) {
      // we don't offer a way to take advantage fo kafka implementation detail to decide which data
      // directory the replica should be. This kind of usage might make the executor complicate, and
      // it is probably rarely used. Also, not offering much value to the problem.
      Objects.requireNonNull(toDir, "The destination data directory must be specified explicitly");

      var topicPartition = TopicPartition.of(replica.topic(), replica.partition());
      var theReplica =
          logPlacements(topicPartition).stream()
              .filter(r -> r.topicPartitionReplica().equals(replica))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("No such replica: " + replica));
      var newReplica = this.update(theReplica, toBroker, toDir);

      return new ClusterLogAllocationImpl(
          allocation.values().stream()
              .flatMap(Collection::stream)
              .map(r -> r == theReplica ? newReplica : r)
              .collect(Collectors.toUnmodifiableList()));
    }

    @Override
    public ClusterLogAllocation becomeLeader(TopicPartitionReplica replica) {
      final var topicPartition = TopicPartition.of(replica.topic(), replica.partition());
      final var source =
          logPlacements(topicPartition).stream()
              .filter(r -> r.topicPartitionReplica().equals(replica))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("No such replica: " + replica));
      final var target =
          logPlacements(topicPartition).stream()
              .filter(Replica::isPreferredLeader)
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No preferred leader found for "
                              + topicPartition
                              + ", this replica list is probably corrupted."));

      final var newSource = this.update(source, true);
      final var newTarget = this.update(target, false);

      return new ClusterLogAllocationImpl(
          allocation.values().stream()
              .flatMap(Collection::stream)
              .map(r -> (r == source ? newSource : (r == target ? newTarget : (r))))
              .collect(Collectors.toUnmodifiableList()));
    }

    @Override
    public Set<Replica> logPlacements(TopicPartition topicPartition) {
      return allocation.getOrDefault(topicPartition, Set.of());
    }

    @Override
    public Set<Replica> logPlacements() {
      return allocation.values().stream()
          .flatMap(Collection::stream)
          .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
      return allocation.keySet();
    }

    private Replica update(Replica source, int newBroker, String newDir) {
      // lookup nodeInfo
      final var theNodeInfo =
          allocation.values().stream()
              .flatMap(Collection::stream)
              .map(ReplicaInfo::nodeInfo)
              .filter(x -> x.id() == newBroker)
              .findFirst();

      return theNodeInfo
          .map(info -> ClusterLogAllocation.update(source, info, newDir))
          .orElseGet(() -> ClusterLogAllocation.update(source, newBroker, newDir));
    }

    private Replica update(Replica source, boolean isPreferredLeader) {
      return Replica.of(
          source.topic(),
          source.partition(),
          source.nodeInfo(),
          source.lag(),
          source.size(),
          source.isLeader(),
          source.inSync(),
          source.isFuture(),
          source.isOffline(),
          isPreferredLeader,
          source.path());
    }
  }

  static Replica update(Replica source, int newBroker, String newDir) {
    if (source.nodeInfo().id() == newBroker) return update(source, source.nodeInfo(), newDir);
    else return update(source, NodeInfo.of(newBroker, "?", -1), newDir);
  }

  static Replica update(Replica source, NodeInfo newBroker, String newDir) {
    return Replica.builder(source).nodeInfo(newBroker).path(newDir).build();
  }
}
