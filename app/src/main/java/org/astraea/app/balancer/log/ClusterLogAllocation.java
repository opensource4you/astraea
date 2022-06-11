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

  /** let specific broker leave the replica set and let another broker join the replica set. */
  void migrateReplica(TopicPartition topicPartition, int atBroker, int toBroker);

  /** let specific follower log become the leader log of this topic/partition. */
  void letReplicaBecomeLeader(TopicPartition topicPartition, int followerReplica);

  /** change the data directory of specific log */
  void changeDataDirectory(TopicPartition topicPartition, int atBroker, String newPath);

  /** Retrieve the log placements of specific {@link TopicPartition}. */
  List<LogPlacement> logPlacements(TopicPartition topicPartition);

  /** Retrieve the stream of all topic/partition pairs in allocation. */
  Stream<TopicPartition> topicPartitionStream();

  static Set<TopicPartition> findNonFulfilledAllocation(
      ClusterLogAllocation source, ClusterLogAllocation target) {

    final var targetTopicPartition =
        target.topicPartitionStream().collect(Collectors.toUnmodifiableSet());

    final var disappearedTopicPartitions =
        source
            .topicPartitionStream()
            .filter(sourceTp -> !targetTopicPartition.contains(sourceTp))
            .collect(Collectors.toUnmodifiableSet());

    if (!disappearedTopicPartitions.isEmpty())
      throw new IllegalArgumentException(
          "Some of the topic/partitions in source allocation is disappeared in the target allocation. Balancer can't do topic deletion or shrinking partition size: "
              + disappearedTopicPartitions);

    return source
        .topicPartitionStream()
        .filter(tp -> !LogPlacement.isMatch(source.logPlacements(tp), target.logPlacements(tp)))
        .collect(Collectors.toUnmodifiableSet());
  }

  static String describeAllocation(ClusterLogAllocation allocation) {
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
