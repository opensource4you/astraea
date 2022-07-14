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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.TopicPartition;

/**
 * Describe the log allocation state of a Kafka cluster. The implementation have to keep the cluster
 * log allocation information, provide method for query the placement, and offer a set of log
 * placement change operation.
 */
public interface ClusterLogAllocation {

  /**
   * let specific broker leave the replica set and let another broker join the replica set. Which
   * data directory the migrated replica will be is up to the Kafka broker implementation to decide.
   *
   * @param topicPartition the topic/partition to perform replica migration
   * @param atBroker the id of the broker about to remove
   * @param toBroker the id of the broker about to replace the removed broker
   */
  default void migrateReplica(TopicPartition topicPartition, int atBroker, int toBroker) {
    migrateReplica(topicPartition, atBroker, toBroker, null);
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
  void migrateReplica(TopicPartition topicPartition, int atBroker, int toBroker, String toDir);
  // TODO: Revise the log argument by TopicPartitionReplica, once #411 is merged

  /** let specific follower log become the leader log of this topic/partition. */
  void letReplicaBecomeLeader(TopicPartition topicPartition, int followerReplica);

  /** Retrieve the log placements of specific {@link TopicPartition}. */
  List<LogPlacement> logPlacements(TopicPartition topicPartition);

  /** Retrieve the stream of all topic/partition pairs in allocation. */
  Stream<TopicPartition> topicPartitionStream();

  /**
   * Find a subset of topic/partitions in the source allocation, that has any non-fulfilled log
   * placement in the given target allocation. Note that the given two allocations must have the
   * exactly same topic/partitions set. Otherwise, an {@link IllegalArgumentException} will be
   * raised.
   */
  static Set<TopicPartition> findNonFulfilledAllocation(
      ClusterLogAllocation source, ClusterLogAllocation target) {

    final var sourceTopicPartition =
        source.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
    final var targetTopicPartition =
        target.topicPartitionStream().collect(Collectors.toUnmodifiableSet());

    if (!sourceTopicPartition.equals(targetTopicPartition))
      throw new IllegalArgumentException(
          "source allocation and target allocation has different topic/partition set");

    return sourceTopicPartition.stream()
        .filter(tp -> !LogPlacement.isMatch(source.logPlacements(tp), target.logPlacements(tp)))
        .collect(Collectors.toUnmodifiableSet());
  }

  static String toString(ClusterLogAllocation allocation) {
    StringBuilder stringBuilder = new StringBuilder();

    allocation
        .topicPartitionStream()
        .sorted()
        .forEach(
            tp -> {
              stringBuilder.append("[").append(tp).append("] ");

              allocation
                  .logPlacements(tp)
                  .forEach(
                      log ->
                          stringBuilder.append(
                              String.format("%s(%s) ", log.broker(), log.logDirectory())));

              stringBuilder.append(System.lineSeparator());
            });

    return stringBuilder.toString();
  }
}
