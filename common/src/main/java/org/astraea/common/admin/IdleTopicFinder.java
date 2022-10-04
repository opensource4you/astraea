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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * To find idle topics defined by user. The user add "IdleTopicChecker" to filter all non-internal
 * topics.
 *
 * <pre>{@code
 * try (Admin admin = Admin.of("localhost:9092")) {
 *   var finder = admin.idleTopicFinder();
 *   Set<String> idleTopics = finder.idleTopics();
 * }
 * }</pre>
 *
 * By default, the finder determines the idle topics by consumer group assignment and timestamp of
 * partition's latest offset. To "add" or "clear" the restriction of idle topic, use {@link
 * IdleTopicFinder#addChecker(Checker)} or {@link IdleTopicFinder#clearChecker()}
 *
 * <pre>{@code
 * try (Admin admin = Admin.of("localhost:9092")) {
 *   var finder = admin.idleTopicFinder();
 *   finder.clearChecker();
 *   finder.addChecker(IdleTopicFinder.IdleTopicChecker.noAssignment());
 *   Set<String> idleTopics = finder.idleTopics();
 * }
 * }</pre>
 */
public class IdleTopicFinder {
  private final Admin admin;

  private final List<Checker> checkers = new ArrayList<>();

  public IdleTopicFinder(Admin admin) {
    this.admin = admin;
    addChecker(Checker.noAssignment());
    addChecker(Checker.latestTimestamp(Duration.ofSeconds(10)));
  }

  public Set<String> idleTopics() {
    if (checkers.isEmpty()) {
      throw new RuntimeException("Can not check for idle topics because of no checkers!");
    }
    var checkerResults =
        checkers.stream()
            .map(checker -> checker.idleTopics(this.admin))
            .collect(Collectors.toList());
    var topicUnion =
        checkerResults.stream()
            .reduce(
                new HashSet<>(),
                (s1, s2) -> {
                  s1.addAll(s2);
                  return s1;
                });
    return checkerResults.stream()
        .reduce(
            topicUnion,
            (s1, s2) -> {
              s1.forEach(
                  topic -> {
                    if (!s2.contains(topic)) s1.remove(topic);
                  });
              return s1;
            });
  }

  public void addChecker(Checker checker) {
    this.checkers.add(checker);
  }

  public void clearChecker() {
    this.checkers.clear();
  }

  public interface Checker {
    Set<String> idleTopics(Admin admin);

    /** Find topics which is **not** assigned by any consumer. */
    static Checker noAssignment() {
      return admin -> {
        var topicNames = admin.topicNames(false);
        var notIdleTopic =
            // TODO: consumer may not belong to any consumer group
            admin.consumerGroups(admin.consumerGroupIds()).stream()
                .flatMap(
                    group ->
                        group.assignment().values().stream()
                            .flatMap(Collection::stream)
                            .map(TopicPartition::topic))
                .collect(Collectors.toUnmodifiableSet());

        return topicNames.stream()
            .filter(name -> !notIdleTopic.contains(name))
            .collect(Collectors.toUnmodifiableSet());
      };
    }

    // TODO: Timestamp may custom by producer, maybe check the time by idempotent state. See:
    // https://github.com/skiptests/astraea/issues/739#issuecomment-1254838359
    static Checker latestTimestamp(Duration duration) {
      return admin -> {
        var topicNames = admin.topicNames(false);
        long now = System.currentTimeMillis();
        return admin.partitions(topicNames).stream()
            .collect(
                Collectors.groupingBy(
                    Partition::topic,
                    Collectors.maxBy((p1, p2) -> (int) (p1.maxTimestamp() - p2.maxTimestamp()))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .filter(p -> p.get().maxTimestamp() < now - duration.toMillis())
            .map(p -> p.get().topic())
            .collect(Collectors.toUnmodifiableSet());
      };
    }
  }
}
