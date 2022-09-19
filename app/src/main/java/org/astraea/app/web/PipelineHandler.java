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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Member;
import org.astraea.common.admin.ProducerState;

class PipelineHandler implements Handler {

  static final String ACTIVE_KEY = "active";

  private final Admin admin;

  PipelineHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Response get(Channel channel) {
    var tps =
        topicPartitions(admin).stream()
            .filter(filter(channel.queries()))
            .collect(Collectors.toUnmodifiableList());
    return new TopicPartitions(tps);
  }

  static Predicate<TopicPartition> filter(Map<String, String> queries) {
    var flag = queries.get(ACTIVE_KEY);
    // remove the topic-partitions having no producers/consumers
    if (flag != null && flag.equalsIgnoreCase("true"))
      return tp -> !tp.to.isEmpty() || !tp.from.isEmpty();
    // remove the topic-partitions having producers/consumers
    if (flag != null && flag.equalsIgnoreCase("false"))
      return tp -> tp.to.isEmpty() && tp.from.isEmpty();
    return tp -> true;
  }

  static Collection<TopicPartition> topicPartitions(Admin admin) {
    var result =
        admin.partitions().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), tp -> new TopicPartition(tp.topic(), tp.partition())));
    admin
        .consumerGroups()
        .values()
        .forEach(
            cg ->
                cg.assignment()
                    .forEach(
                        (m, tps) ->
                            tps.stream()
                                // it could return new topic partition, so we have to remove them.
                                .filter(result::containsKey)
                                .forEach(tp -> result.get(tp).to.add(new Consumer(m)))));
    admin
        .producerStates(result.keySet())
        .forEach((tp, p) -> p.forEach(s -> result.get(tp).from.add(new Producer(s))));
    return result.values().stream()
        .sorted(
            Comparator.comparing((TopicPartition tp) -> tp.topic).thenComparing(tp -> tp.partition))
        .collect(Collectors.toUnmodifiableList());
  }

  static class Producer implements Response {
    final long producerId;
    final int producerEpoch;
    final int lastSequence;
    final long lastTimestamp;

    Producer(ProducerState state) {
      this.producerId = state.producerId();
      this.producerEpoch = state.producerEpoch();
      this.lastSequence = state.lastSequence();
      this.lastTimestamp = state.lastTimestamp();
    }
  }

  static class Consumer implements Response {
    final String groupId;
    final String memberId;
    final String groupInstanceId;
    final String clientId;
    final String host;

    Consumer(Member member) {
      this.groupId = member.groupId();
      this.memberId = member.memberId();
      this.groupInstanceId = member.groupInstanceId().orElse(null);
      this.clientId = member.clientId();
      this.host = member.host();
    }
  }

  static class TopicPartition implements Response {
    final String topic;
    final int partition;
    final Collection<Producer> from = new ArrayList<>();
    final Collection<Consumer> to = new ArrayList<>();

    TopicPartition(String topic, int partition) {
      this.topic = topic;
      this.partition = partition;
    }
  }

  static class TopicPartitions implements Response {
    final Collection<TopicPartition> topicPartitions;

    TopicPartitions(Collection<TopicPartition> topicPartitions) {
      this.topicPartitions = topicPartitions;
    }
  }
}
