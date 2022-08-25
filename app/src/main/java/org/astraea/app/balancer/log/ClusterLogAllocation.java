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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;

/**
 * Describe the log allocation state of a Kafka cluster. The implementation have to keep the cluster
 * log allocation information, provide method for query the placement, and offer a set of log
 * placement change operation.
 */
public interface ClusterLogAllocation {

  static ClusterLogAllocation of(ClusterInfo clusterInfo) {
    return of(
        clusterInfo.replicas().stream()
            .filter(r -> r instanceof Replica)
            .map(r -> (Replica) r)
            .collect(
                Collectors.groupingBy(
                    replica ->
                        TopicPartition.of(replica.topic(), Integer.toString(replica.partition()))))
            .entrySet()
            .stream()
            .map(
                entry -> {
                  // validate if the given log placements are valid
                  if (entry.getValue().stream().filter(ReplicaInfo::isLeader).count() != 1)
                    throw new IllegalArgumentException(
                        "The " + entry.getKey() + " leader count mismatch 1.");

                  final var topicPartition = entry.getKey();
                  final var logPlacements =
                      entry.getValue().stream()
                          .sorted(Comparator.comparingInt(replica -> replica.isLeader() ? 0 : 1))
                          .map(
                              replica ->
                                  LogPlacement.of(replica.nodeInfo().id(), replica.dataFolder()))
                          .collect(Collectors.toList());

                  return Map.entry(topicPartition, logPlacements);
                })
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  static ClusterLogAllocation of(Map<TopicPartition, List<LogPlacement>> allocation) {
    return new ClusterLogAllocationImpl(allocation);
  }
  /**
   * let specific broker leave the replica set and let another broker join the replica set. Which
   * data directory the migrated replica will be is up to the Kafka broker implementation to decide.
   *
   * @param topicPartition the topic/partition to perform replica migration
   * @param atBroker the id of the broker about to remove
   * @param toBroker the id of the broker about to replace the removed broker
   */
  default ClusterLogAllocation migrateReplica(
      TopicPartition topicPartition, int atBroker, int toBroker) {
    return migrateReplica(topicPartition, atBroker, toBroker, null);
  }
  // TODO: Revise the log argument by TopicPartitionReplica, once #411 is merged

  /**
   * let specific broker leave the replica set and let another broker join the replica set.
   *
   * @param topicPartition the topic/partition to perform replica migration
   * @param atBroker the id of the broker about to remove
   * @param toBroker the id of the broker about to replace the removed broker
   * @param toDir the absolute path of the data directory this migrated replica is supposed to be on
   *     the destination broker, if {@code null} is specified then the data directory choice is left
   *     up to the Kafka broker implementation.
   */
  ClusterLogAllocation migrateReplica(
      TopicPartition topicPartition, int atBroker, int toBroker, String toDir);
  // TODO: Revise the log argument by TopicPartitionReplica, once #411 is merged

  /** let specific follower log become the leader log of this topic/partition. */
  ClusterLogAllocation letReplicaBecomeLeader(TopicPartition topicPartition, int followerReplica);

  /**
   * Retrieve the log placements of specific {@link TopicPartition}.
   *
   * @param topicPartition to query
   * @return log placements or empty collection if there is no log placements
   */
  List<LogPlacement> logPlacements(TopicPartition topicPartition);

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

    if (!sourceTopicPartition.equals(targetTopicPartition))
      throw new IllegalArgumentException(
          "source allocation and target allocation has different topic/partition set");

    return sourceTopicPartition.stream()
        .filter(tp -> !LogPlacement.isMatch(source.logPlacements(tp), target.logPlacements(tp)))
        .collect(Collectors.toUnmodifiableSet());
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
                              String.format("(%s, %s) ", log.broker(), log.dataFolder())));

              stringBuilder.append(System.lineSeparator());
            });

    return stringBuilder.toString();
  }

  class ClusterLogAllocationImpl implements ClusterLogAllocation {

    private final Map<TopicPartition, List<LogPlacement>> allocation;

    private ClusterLogAllocationImpl(Map<TopicPartition, List<LogPlacement>> allocation) {
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
            long uniqueBrokers = logs.stream().map(LogPlacement::broker).distinct().count();
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
        TopicPartition topicPartition, int atBroker, int toBroker, String toDir) {
      var sourceLogPlacements = this.logPlacements(topicPartition);
      if (sourceLogPlacements.isEmpty())
        throw new IllegalMigrationException(
            topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");

      int sourceLogIndex = indexOfBroker(sourceLogPlacements, atBroker).orElse(-1);
      if (sourceLogIndex == -1)
        throw new IllegalMigrationException(
            atBroker + " is not part of the replica set for " + topicPartition);

      var newAllocations = new HashMap<>(allocation);
      newAllocations.put(
          topicPartition,
          IntStream.range(0, sourceLogPlacements.size())
              .mapToObj(
                  index ->
                      index == sourceLogIndex
                          ? LogPlacement.of(toBroker, toDir)
                          : sourceLogPlacements.get(index))
              .collect(Collectors.toUnmodifiableList()));
      return new ClusterLogAllocationImpl(newAllocations);
    }

    @Override
    public ClusterLogAllocation letReplicaBecomeLeader(
        TopicPartition topicPartition, int followerReplica) {
      final List<LogPlacement> sourceLogPlacements = this.logPlacements(topicPartition);
      if (sourceLogPlacements.isEmpty())
        throw new IllegalMigrationException(
            topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");

      int leaderLogIndex = 0;
      int followerLogIndex = indexOfBroker(sourceLogPlacements, followerReplica).orElse(-1);
      if (followerLogIndex == -1)
        throw new IllegalArgumentException(
            followerReplica + " is not part of the replica set for " + topicPartition);

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
    public List<LogPlacement> logPlacements(TopicPartition topicPartition) {
      return allocation.getOrDefault(topicPartition, List.of());
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
      return allocation.keySet();
    }

    private static OptionalInt indexOfBroker(List<LogPlacement> logPlacements, int targetBroker) {
      return IntStream.range(0, logPlacements.size())
          .filter(index -> logPlacements.get(index).broker() == targetBroker)
          .findFirst();
    }
  }
}
