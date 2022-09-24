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
package org.astraea.app.balancer.log;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
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
 * Describe the log allocation state of a Kafka cluster. The implementation have to keep the cluster
 * log allocation information, provide method for query the placement, and offer a set of log
 * placement change operation.
 */
public interface ClusterLogAllocation {

  static ClusterLogAllocation of(ClusterInfo<Replica> clusterInfo) {
    return of(
        clusterInfo
            .replicaStream()
            .collect(Collectors.groupingBy(r -> TopicPartition.of(r.topic(), r.partition())))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      // validate if the given log placements are valid
                      if (entry.getValue().stream().filter(ReplicaInfo::isLeader).count() != 1)
                        throw new IllegalArgumentException(
                            "The " + entry.getKey() + " leader count mismatch 1.");
                      return entry.getValue().stream()
                          .sorted(Comparator.comparingInt(replica -> replica.isLeader() ? 0 : 1))
                          .collect(Collectors.toList());
                    })));
  }

  static ClusterLogAllocation of(Map<TopicPartition, List<Replica>> allocation) {
    return new ClusterLogAllocationImpl(allocation);
  }
  /**
   * let specific broker leave the replica set and let another broker join the replica set. Which
   * data directory the migrated replica will be is up to the Kafka broker implementation to decide.
   *
   * @param replica the replica to perform replica migration
   * @param toBroker the id of the broker about to replace the removed broker
   */
  default ClusterLogAllocation migrateReplica(TopicPartitionReplica replica, int toBroker) {
    return migrateReplica(replica, toBroker, null);
  }

  /**
   * let specific broker leave the replica set and let another broker join the replica set.
   *
   * @param replica the replica to perform replica migration
   * @param toBroker the id of the broker about to replace the removed broker
   * @param toDir the absolute path of the data directory this migrated replica is supposed to be on
   *     the destination broker, if {@code null} is specified then the data directory choice is left
   *     up to the Kafka broker implementation.
   */
  ClusterLogAllocation migrateReplica(TopicPartitionReplica replica, int toBroker, String toDir);

  /** let specific follower log become the leader log of this topic/partition. */
  ClusterLogAllocation letReplicaBecomeLeader(TopicPartitionReplica replica);

  /**
   * Retrieve the log placements of specific {@link TopicPartition}.
   *
   * @param topicPartition to query
   * @return log placements or empty collection if there is no log placements
   */
  List<Replica> logPlacements(TopicPartition topicPartition);

  /** Retrieve the stream of all topic/partition pairs in allocation. */
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
        .filter(tp -> !replicaListEqual(source.logPlacements(tp), target.logPlacements(tp)))
        .collect(Collectors.toUnmodifiableSet());
  }

  static boolean replicaListEqual(List<Replica> sourceReplicas, List<Replica> targetReplicas) {
    if (sourceReplicas.size() != targetReplicas.size()) return false;
    return IntStream.range(0, sourceReplicas.size())
        .allMatch(
            index ->
                sourceReplicas.get(index).nodeInfo().id()
                        == targetReplicas.get(index).nodeInfo().id()
                    && sourceReplicas
                        .get(index)
                        .dataFolder()
                        .equals(targetReplicas.get(index).dataFolder()));
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
                              String.format("(%s, %s) ", log.nodeInfo().id(), log.dataFolder())));

              stringBuilder.append(System.lineSeparator());
            });

    return stringBuilder.toString();
  }

  class ClusterLogAllocationImpl implements ClusterLogAllocation {

    private final Map<TopicPartition, List<Replica>> allocation;

    private ClusterLogAllocationImpl(Map<TopicPartition, List<Replica>> allocation) {
      this.allocation = Collections.unmodifiableMap(allocation);

      this.allocation.keySet().stream()
          .collect(Collectors.groupingBy(TopicPartition::topic))
          .forEach(
              (topic, tp) -> {
                int maxPartitionId =
                    tp.stream().mapToInt(TopicPartition::partition).max().orElseThrow();
                if ((maxPartitionId + 1) != tp.size())
                  throw new IllegalArgumentException(
                      "The partition size of " + topic + " is illegal");
              });

      this.allocation.forEach(
          (tp, logs) -> {
            long uniqueBrokers =
                logs.stream().map(ReplicaInfo::nodeInfo).mapToInt(NodeInfo::id).distinct().count();
            if (uniqueBrokers != logs.size() || logs.size() == 0)
              throw new IllegalArgumentException(
                  "The topic "
                      + tp.topic()
                      + " partition "
                      + tp.partition()
                      + " has illegal replica set "
                      + logs);
          });
    }

    @Override
    public ClusterLogAllocation migrateReplica(
        TopicPartitionReplica replica, int toBroker, String toDir) {
      var topicPartition = TopicPartition.of(replica.topic(), replica.partition());
      var sourceLogPlacements = this.logPlacements(topicPartition);
      if (sourceLogPlacements.isEmpty())
        throw new IllegalMigrationException(
            replica.topic() + "-" + replica.partition() + " no such topic/partition");

      int sourceLogIndex = indexOfBroker(sourceLogPlacements, replica.brokerId()).orElse(-1);
      if (sourceLogIndex == -1)
        throw new IllegalMigrationException(
            replica.brokerId() + " is not part of the replica set for " + topicPartition);

      var newAllocations = new HashMap<>(allocation);
      newAllocations.put(
          topicPartition,
          IntStream.range(0, sourceLogPlacements.size())
              .mapToObj(
                  index ->
                      index == sourceLogIndex
                          ? this.update(sourceLogPlacements.get(index), toBroker, toDir)
                          : sourceLogPlacements.get(index))
              .collect(Collectors.toUnmodifiableList()));
      return new ClusterLogAllocationImpl(newAllocations);
    }

    @Override
    public ClusterLogAllocation letReplicaBecomeLeader(TopicPartitionReplica replica) {
      final var topicPartition = TopicPartition.of(replica.topic(), replica.partition());
      final var sourceLogPlacements = this.logPlacements(topicPartition);
      if (sourceLogPlacements.isEmpty())
        throw new IllegalMigrationException(
            replica.topic() + "-" + replica.partition() + " no such topic/partition");

      int leaderLogIndex = 0;
      int followerLogIndex = indexOfBroker(sourceLogPlacements, replica.brokerId()).orElse(-1);
      if (followerLogIndex == -1)
        throw new IllegalArgumentException(
            replica.brokerId() + " is not part of the replica set for " + topicPartition);

      if (leaderLogIndex == followerLogIndex) return this; // nothing to do

      final var leaderLog = this.logPlacements(topicPartition).get(leaderLogIndex);
      final var followerLog = this.logPlacements(topicPartition).get(followerLogIndex);

      var newAllocations = new HashMap<>(allocation);
      newAllocations.put(
          topicPartition,
          IntStream.range(0, sourceLogPlacements.size())
              .mapToObj(
                  index ->
                      index == leaderLogIndex
                          ? followerLog
                          : index == followerLogIndex ? leaderLog : sourceLogPlacements.get(index))
              .collect(Collectors.toUnmodifiableList()));

      return new ClusterLogAllocationImpl(newAllocations);
    }

    @Override
    public List<Replica> logPlacements(TopicPartition topicPartition) {
      return allocation.getOrDefault(topicPartition, List.of());
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
      return allocation.keySet();
    }

    private static OptionalInt indexOfBroker(List<Replica> logPlacements, int targetBroker) {
      return IntStream.range(0, logPlacements.size())
          .filter(index -> logPlacements.get(index).nodeInfo().id() == targetBroker)
          .findFirst();
    }

    Replica update(Replica source, int newBroker, String newDir) {
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
  }

  static Replica update(Replica source, int newBroker, String newDir) {
    if (source.nodeInfo().id() == newBroker) return update(source, source.nodeInfo(), newDir);
    else return update(source, NodeInfo.of(newBroker, "?", -1), newDir);
  }

  static Replica update(Replica source, NodeInfo newBroker, String newDir) {
    return Replica.of(
        source.topic(),
        source.partition(),
        newBroker,
        source.lag(),
        source.size(),
        source.isLeader(),
        source.inSync(),
        source.isFuture(),
        source.isOffline(),
        source.isPreferredLeader(),
        newDir);
  }
}
