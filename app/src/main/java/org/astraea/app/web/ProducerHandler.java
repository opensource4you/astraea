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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.TopicPartition;

class ProducerHandler implements Handler {

  static final String TOPIC_KEY = "topic";
  static final String PARTITION_KEY = "partition";

  private final AsyncAdmin admin;

  ProducerHandler(AsyncAdmin admin) {
    this.admin = admin;
  }

  CompletionStage<Set<TopicPartition>> partitions(Map<String, String> queries) {
    if (queries.containsKey(TOPIC_KEY) && queries.containsKey(PARTITION_KEY))
      return CompletableFuture.completedStage(
          Set.of(TopicPartition.of(queries.get(TOPIC_KEY), queries.get(PARTITION_KEY))));
    if (queries.containsKey(TOPIC_KEY))
      return admin.topicPartitions(Set.of(queries.get(TOPIC_KEY)));
    return admin.topicNames(false).thenCompose(admin::topicPartitions);
  }

  @Override
  public CompletionStage<Partitions> get(Channel channel) {
    return partitions(channel.queries())
        .thenCompose(admin::producerStates)
        .thenApply(
            producerStates ->
                new Partitions(
                    producerStates.stream()
                        .collect(
                            Collectors.groupingBy(
                                org.astraea.common.admin.ProducerState::topicPartition))
                        .entrySet()
                        .stream()
                        .map(e -> new Partition(e.getKey(), e.getValue()))
                        .collect(Collectors.toUnmodifiableList())));
  }

  static class ProducerState implements Response {

    final long producerId;
    final int producerEpoch;
    final int lastSequence;
    final long lastTimestamp;

    ProducerState(org.astraea.common.admin.ProducerState state) {
      this.producerId = state.producerId();
      this.producerEpoch = state.producerEpoch();
      this.lastSequence = state.lastSequence();
      this.lastTimestamp = state.lastTimestamp();
    }
  }

  static class Partition implements Response {
    final String topic;
    final int partition;
    final List<ProducerState> states;

    Partition(
        org.astraea.common.admin.TopicPartition tp,
        Collection<org.astraea.common.admin.ProducerState> states) {
      this.topic = tp.topic();
      this.partition = tp.partition();
      this.states =
          states.stream().map(ProducerState::new).collect(Collectors.toUnmodifiableList());
    }
  }

  static class Partitions implements Response {
    final List<Partition> partitions;

    Partitions(List<Partition> partitions) {
      this.partitions = partitions;
    }
  }
}
