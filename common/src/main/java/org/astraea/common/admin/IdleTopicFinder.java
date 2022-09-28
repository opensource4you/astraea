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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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
 * IdleTopicFinder#addChecker(IdleTopicChecker)} or {@link IdleTopicFinder#clearChecker()}
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

  private final List<IdleTopicChecker> checkers = new ArrayList<>();

  public IdleTopicFinder(Admin admin) {
    this.admin = admin;
    addChecker(IdleTopicChecker.noAssignment());
    addChecker(IdleTopicChecker.latestTimestamp(Duration.ofSeconds(10)));
  }

  public Set<String> idleTopics() {
    if (checkers.isEmpty()) {
      throw new RuntimeException("Can not check for idle topics because of no checkers!");
    }

    var topicNames = admin.topicNames();
    checkers.forEach(c -> c.fetchLatest(admin));
    return topicNames.stream()
        .filter(topic -> checkers.stream().allMatch(checker -> checker.test(admin, topic)))
        .collect(Collectors.toUnmodifiableSet());
  }

  public void addChecker(IdleTopicChecker checker) {
    this.checkers.add(checker);
  }

  public void clearChecker() {
    this.checkers.clear();
  }

  public interface IdleTopicChecker {

    void fetchLatest(Admin admin);

    boolean test(Admin admin, String topicName);

    /** Find topics which is **not** assigned by any consumer. */
    static IdleTopicChecker noAssignment() {
      return new IdleTopicChecker() {
        private final AtomicReference<Set<String>> idleTopicsCache = new AtomicReference<>();

        @Override
        public void fetchLatest(Admin admin) {
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

          idleTopicsCache.set(
              topicNames.stream()
                  .filter(name -> !notIdleTopic.contains(name))
                  .collect(Collectors.toUnmodifiableSet()));
        }

        @Override
        public boolean test(Admin admin, String topicName) {
          if (idleTopicsCache.get() == null)
            throw new NullPointerException(
                "IdleTopicChecker.fetchLatest() should be called before checking topic name.");
          return idleTopicsCache.get().contains(topicName);
        }
      };
    }

    // TODO: Timestamp may custom by producer, maybe check the time by idempotent state. See:
    // https://github.com/skiptests/astraea/issues/739#issuecomment-1254838359
    static IdleTopicChecker latestTimestamp(Duration duration) {
      return new IdleTopicChecker() {
        private final AtomicReference<Set<String>> idleTopicsCache = new AtomicReference<>();

        @Override
        public void fetchLatest(Admin admin) {
          var topicNames = admin.topicNames(false);
          long now = System.currentTimeMillis();
          idleTopicsCache.set(
              admin.partitions(topicNames).stream()
                  .collect(
                      Collectors.groupingBy(
                          Partition::topic,
                          Collectors.maxBy(
                              (p1, p2) -> (int) (p1.maxTimestamp() - p2.maxTimestamp()))))
                  .values()
                  .stream()
                  .filter(Optional::isPresent)
                  .filter(p -> p.get().maxTimestamp() < now - duration.toMillis())
                  .map(p -> p.get().topic())
                  .collect(Collectors.toUnmodifiableSet()));
        }

        @Override
        public boolean test(Admin admin, String topicName) {
          if (idleTopicsCache.get() == null)
            throw new NullPointerException(
                "IdleTopicChecker.fetchLatest() should be called before checking topic name.");
          return idleTopicsCache.get().contains(topicName);
        }
      };
    }
  }
}
