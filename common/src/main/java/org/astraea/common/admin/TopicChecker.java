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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Argument for {@link AsyncAdmin#idleTopic(List)}. This interface will check for the given set of
 * topic names, and filter out the "idle" topics.
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
  CompletionStage<Set<String>> usedTopics(AsyncAdmin admin, Set<String> topics);

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

  /** Find topics whose latest record timestamp is not older than the given duration. */
  // TODO: Timestamp may custom by producer, maybe check the time by idempotent state. See:
  // https://github.com/skiptests/astraea/issues/739#issuecomment-1254838359
  static TopicChecker latestTimestamp(Duration duration) {
    return (admin, topics) -> {
      long now = System.currentTimeMillis();
      return admin
          .partitions(topics)
          .thenApply(
              partitions ->
                  // Filtering the topic to get the topic that
                  // 1. offset of each partition are not 0
                  // 2. the max timestamp is not smaller than the given time
                  //    or
                  //    the topic does not support max timestamp
                  partitions.stream()
                      .filter(p -> p.latestOffset() > 0)
                      .collect(
                          Collectors.groupingBy(
                              Partition::topic,
                              Collectors.maxBy(Comparator.comparingLong(Partition::maxTimestamp))))
                      .values()
                      .stream()
                      .filter(Optional::isPresent)
                      .filter(
                          p ->
                              now - duration.toMillis() < p.get().maxTimestamp()
                                  || p.get().maxTimestamp() == -1)
                      .map(p -> p.get().topic())
                      .collect(Collectors.toUnmodifiableSet()));
    };
  }
}
