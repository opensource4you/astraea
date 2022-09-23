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
package org.astraea.app.web;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;

public class IdleTopicHandler implements Handler {
  static final String DURATION_KEY = "duration";

  private final Admin admin;
  private final String bootstrapServer;

  public IdleTopicHandler(Admin admin, String bootstrapServer) {
    this.admin = admin;
    this.bootstrapServer = bootstrapServer;
  }

  @Override
  public Response get(Channel channel) {
    Duration duration = Duration.parse(channel.queries().get(DURATION_KEY));

    Set<String> consumerIdle = consumeIdleTopic();
    Set<String> produceIdle = produceIdleTopic(duration);

    return IdleTopics.of(
        produceIdle.stream()
            .filter(consumerIdle::contains)
            .collect(Collectors.toUnmodifiableSet()));
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

  private Map<String, Long> latestTimeStamp(Set<String> topicNames) {
    var partitionOffset = admin.offsets(topicNames);
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
      var kafkaPartitionOffset =
          partitionOffset.entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      e ->
                          new org.apache.kafka.common.TopicPartition(
                              e.getKey().topic(), e.getKey().partition()),
                      e -> (e.getValue().latest() - 1 < 0) ? 0 : e.getValue().latest() - 1));

      var latestPerTopic = new HashMap<String, Long>();
      kafkaPartitionOffset.forEach(
          (tp, offset) -> {
            consumer.assign(Set.of(tp));
            consumer.seek(tp, offset);
            consumer
                .poll(Duration.ofMillis(100))
                .forEach(
                    record -> {
                      latestPerTopic.computeIfPresent(
                          tp.topic(), (k, v) -> v < record.timestamp() ? record.timestamp() : v);
                      latestPerTopic.computeIfAbsent(tp.topic(), ignore -> record.timestamp());
                    });
          });
      return latestPerTopic;
    }
  }

  static class IdleTopics implements Response {
    final Set<String> topicNames;

    static IdleTopics of(Set<String> topicNames) {
      return new IdleTopics(topicNames);
    }

    IdleTopics(Set<String> topicNames) {
      this.topicNames = Collections.unmodifiableSet(topicNames);
    }
  }
}
