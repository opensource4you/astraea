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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.consumer.Builder;
import org.astraea.common.consumer.Consumer;

public class IdleTopicFinder implements AutoCloseable {
  private final String bootstrapServers;
  private final Admin admin;

  public IdleTopicFinder(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    this.admin = Admin.of(bootstrapServers);
  }

  public Set<String> idleTopics(Duration duration) {
    Set<String> consumerIdle = consumeIdleTopic();
    Set<String> produceIdle = produceIdleTopic(duration);
    return produceIdle.stream()
        .filter(consumerIdle::contains)
        .collect(Collectors.toUnmodifiableSet());
  }

  // TODO: Timestamp may custom by producer, may be check the time by idempotent state. See:
  // https://github.com/skiptests/astraea/issues/739#issuecomment-1254838359
  Set<String> produceIdleTopic(Duration duration) {
    long now = System.currentTimeMillis();
    Map<String, Long> latestTimeStamp = latestTimeStamp(admin.topicNames(false));

    return latestTimeStamp.entrySet().stream()
        .filter(e -> now - e.getValue() >= duration.toMillis())
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  /** Find topics which is **not** assigned by any consumer. */
  Set<String> consumeIdleTopic() {
    var notIdleTopic =
        // TODO: consumer may not belong to any consumer group
        admin.consumerGroups().entrySet().stream()
            .flatMap(
                topicGroup ->
                    topicGroup.getValue().assignment().values().stream()
                        .flatMap(Collection::stream)
                        .map(TopicPartition::topic))
            .collect(Collectors.toUnmodifiableSet());

    return admin.topicNames(false).stream()
        .filter(name -> !notIdleTopic.contains(name))
        .collect(Collectors.toUnmodifiableSet());
  }

  // This method will build many consumers (as many as the given topic-partitions).
  private Map<String, Long> latestTimeStamp(Set<String> topicNames) {
    var topicPartitions = admin.partitions(topicNames);
    var latestPerTopic = new HashMap<String, Long>();

    topicPartitions.forEach(
        tp -> {
          try (var consumer =
              Consumer.forPartitions(Set.of(tp))
                  .bootstrapServers(bootstrapServers)
                  .seek(Builder.SeekStrategy.DISTANCE_FROM_LATEST, 1)
                  .build()) {
            consumer
                .poll(1, Duration.ofMillis(100))
                .forEach(
                    record -> {
                      latestPerTopic.computeIfPresent(
                          tp.topic(), (ignore, time) -> Math.max(time, record.timestamp()));
                      latestPerTopic.computeIfAbsent(tp.topic(), ignore -> record.timestamp());
                    });
          }
        });

    return latestPerTopic;
  }

  @Override
  public void close() {
    admin.close();
  }
}
