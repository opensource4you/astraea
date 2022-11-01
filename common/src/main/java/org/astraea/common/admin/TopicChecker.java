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
 * Argument for {@link Admin#idleTopic(List)}. This interface will check for the given set of topic
 * names, and filter out the "idle" topics.
 *
 * <p>For example, the definition of "idle" can be "the topic that is not consumed by any consumer".
 * Like {@link #ASSIGNMENT} filter out the topics that/(whose partitions) are not assigned by any
 * consumer group.
 */
@FunctionalInterface
public interface TopicChecker {

  /**
   * @param admin for this checker to cluster information
   * @param topics names of topics this checker will check
   * @return the set of topic names that is not idle
   */
  CompletionStage<Set<String>> usedTopics(Admin admin, Set<String> topics);

  /** Find topics which is assigned by any consumer. */
  TopicChecker ASSIGNMENT =
      (admin, topics) -> {
        var consumerGroups = admin.consumerGroupIds().thenCompose(admin::consumerGroups);

        return consumerGroups.thenApply(
            groups ->
                // TODO: consumer may not belong to any consumer group
                groups.stream()
                    .flatMap(
                        group ->
                            group.assignment().values().stream()
                                .flatMap(Collection::stream)
                                .map(TopicPartition::topic)
                                .filter(topics::contains))
                    .collect(Collectors.toUnmodifiableSet()));
      };

  /**
   * Find topics whose max(the latest record timestamp, producer timestamp, max timestamp of
   * records) is not older than the given duration.
   *
   * @param expired to search expired topics
   * @param timeout to wait the latest record
   */
  static TopicChecker latestTimestamp(Duration expired, Duration timeout) {
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
                      .filter(e -> e.getValue() >= end)
                      .map(e -> e.getKey().topic())
                      .collect(Collectors.toSet()));
    };
  }
}
