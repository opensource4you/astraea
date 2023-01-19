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
package org.astraea.common.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.FutureUtils;

/**
 * Argument for {@link Admin#topicNames(List)}. This interface will check for the given set of topic
 * names, and filter out the "idle" topics.
 *
 * <p>For example, the definition of "idle" can be "the topic that is not consumed by any consumer".
 * Like {@link #NO_CONSUMER_GROUP} filter out the topics that/(whose partitions) are not assigned by
 * any consumer group.
 */
@FunctionalInterface
public interface TopicChecker {

  /**
   * @param admin for this checker to cluster information
   * @param topics names of topics this checker will check
   * @return the set of topic names that is not idle
   */
  CompletionStage<Set<String>> test(Admin admin, Set<String> topics);

  TopicChecker NO_DATA =
      (admin, topics) ->
          admin
              .clusterInfo(topics)
              .thenApply(
                  clusterInfo ->
                      clusterInfo.topics().stream()
                          .filter(
                              t -> clusterInfo.replicaStream(t).mapToLong(Replica::size).sum() <= 0)
                          .collect(Collectors.toSet()));

  /** Find topics which is assigned by any consumer. */
  TopicChecker NO_CONSUMER_GROUP =
      (admin, topics) ->
          admin
              .consumerGroupIds()
              .thenCompose(admin::consumerGroups)
              .thenApply(
                  groups -> {
                    // TODO: consumer may not belong to any consumer group
                    var hasReadTopics =
                        groups.stream()
                            .flatMap(
                                group ->
                                    group.assignment().values().stream()
                                        .flatMap(Collection::stream)
                                        .map(TopicPartition::topic))
                            .collect(Collectors.toSet());
                    return topics.stream()
                        .filter(t -> !hasReadTopics.contains(t))
                        .collect(Collectors.toSet());
                  });

  /**
   * Find out the topic has skew partition size.
   *
   * @param factor the threshold of skew (min size / max size)
   * @return topics having skew partition
   */
  static TopicChecker skewPartition(double factor) {
    return (admin, topics) ->
        admin
            .clusterInfo(topics)
            .thenApply(
                clusterInfo ->
                    clusterInfo.topics().stream()
                        .filter(
                            topic -> {
                              var max =
                                  clusterInfo.replicaLeaders(topic).stream()
                                      .mapToLong(Replica::size)
                                      .max();
                              var min =
                                  clusterInfo.replicaLeaders(topic).stream()
                                      .mapToLong(Replica::size)
                                      .max();
                              return max.isPresent()
                                  && min.isPresent()
                                  && ((double) min.getAsLong() / max.getAsLong() >= factor);
                            })
                        .collect(Collectors.toSet()));
  }

  /**
   * Find topics whose max(the latest record timestamp, producer timestamp, max timestamp of
   * records) is older than the given duration.
   *
   * @param expired to search expired topics
   * @param timeout to wait the latest record
   */
  static TopicChecker noWriteAfter(Duration expired, Duration timeout) {
    return (admin, topics) -> {
      long end = System.currentTimeMillis() - expired.toMillis();
      var tpsFuture = admin.topicPartitions(topics);
      return FutureUtils.combine(
              tpsFuture,
              tpsFuture.thenCompose(tps -> admin.timestampOfLatestRecords(tps, timeout)),
              tpsFuture.thenCompose(admin::producerStates),
              tpsFuture.thenCompose(admin::maxTimestamps),
              (tps, timestampOfLatestRecords, producerStates, maxTimestamps) -> {
                var tpTimeFromState =
                    producerStates.stream()
                        .collect(Collectors.groupingBy(ProducerState::topicPartition))
                        .entrySet()
                        .stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                    e.getValue().stream()
                                        .mapToLong(ProducerState::lastTimestamp)
                                        .max()
                                        .orElse(-1)));
                return tps.stream()
                    .collect(
                        Collectors.toMap(
                            tp -> tp,
                            tp ->
                                Math.max(
                                    timestampOfLatestRecords.getOrDefault(tp, -1L),
                                    Math.max(
                                        tpTimeFromState.getOrDefault(tp, -1L),
                                        maxTimestamps.getOrDefault(tp, -1L)))));
              })
          .thenApply(
              ts ->
                  ts.entrySet().stream()
                      .filter(e -> e.getValue() < end)
                      .map(e -> e.getKey().topic())
                      .collect(Collectors.toSet()));
    };
  }
}
