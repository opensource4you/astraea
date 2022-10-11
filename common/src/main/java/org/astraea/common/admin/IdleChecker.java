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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@FunctionalInterface
public interface IdleChecker {
  CompletionStage<Set<String>> idleTopics(AsyncAdmin admin);

  /** Find topics which is **not** assigned by any consumer. */
  IdleChecker NO_ASSIGNMENT =
      admin -> {
        var groupIds = admin.consumerGroupIds();
        var topicNames = admin.topicNames(false);
        try {
          var consumerGroups = admin.consumerGroups(groupIds.toCompletableFuture().get());

          return topicNames.thenCombine(
              consumerGroups,
              (topics, groups) -> {
                // TODO: consumer may not belong to any consumer group
                var notIdleTopic =
                    groups.stream()
                        .flatMap(
                            group ->
                                group.assignment().values().stream()
                                    .flatMap(Collection::stream)
                                    .map(TopicPartition::topic))
                        .collect(Collectors.toUnmodifiableSet());
                return topics.stream()
                    .filter(name -> !notIdleTopic.contains(name))
                    .collect(Collectors.toUnmodifiableSet());
              });
        } catch (InterruptedException | ExecutionException ex) {
          return CompletableFuture.failedStage(ex);
        }
      };

  /** Find topics whose latest record timestamp is older than the given duration. */
  // TODO: Timestamp may custom by producer, maybe check the time by idempotent state. See:
  // https://github.com/skiptests/astraea/issues/739#issuecomment-1254838359
  static IdleChecker latestTimestamp(Duration duration) {
    return admin -> {
      long now = System.currentTimeMillis();
      var topicNames = admin.topicNames(false);
      try {
        return admin
            .partitions(topicNames.toCompletableFuture().get())
            .thenApply(
                partitions ->
                    partitions.stream()
                        .collect(
                            Collectors.groupingBy(
                                Partition::topic,
                                Collectors.maxBy(
                                    Comparator.comparingLong(Partition::maxTimestamp))))
                        .values()
                        .stream()
                        .filter(Optional::isPresent)
                        .filter(p -> p.get().maxTimestamp() < now - duration.toMillis())
                        .map(p -> p.get().topic())
                        .collect(Collectors.toUnmodifiableSet()));
      } catch (InterruptedException | ExecutionException ex) {
        return CompletableFuture.failedStage(ex);
      }
    };
  }
}
