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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Config;
import org.astraea.app.common.ExecutionRuntimeException;

class TopicHandler implements Handler {

  static final String TOPIC_NAME_KEY = "name";
  static final String NUMBER_OF_PARTITIONS_KEY = "partitions";
  static final String NUMBER_OF_REPLICAS_KEY = "replicas";
  static final String PARTITION_KEY = "partition";
  static final String LIST_INTERNAL = "listInternal";

  private final Admin admin;

  TopicHandler(Admin admin) {
    this.admin = admin;
  }

  Set<String> topicNames(Optional<String> target, boolean listInternal) {
    return Handler.compare(admin.topicNames(listInternal), target);
  }

  @Override
  public Response get(Channel channel) {
    return get(
        topicNames(
            channel.target(),
            Optional.ofNullable(channel.queries().get(LIST_INTERNAL))
                .map(Boolean::parseBoolean)
                .orElse(true)),
        partition ->
            !channel.queries().containsKey(PARTITION_KEY)
                || partition == Integer.parseInt(channel.queries().get(PARTITION_KEY)));
  }

  private Response get(Set<String> topicNames, Predicate<Integer> partitionPredicate) {
    var topics = admin.topics(topicNames);
    var replicas = admin.replicas(topics.keySet());
    var partitions =
        admin.offsets(topics.keySet()).entrySet().stream()
            .filter(e -> partitionPredicate.test(e.getKey().partition()))
            .collect(
                Collectors.groupingBy(
                    e -> e.getKey().topic(),
                    Collectors.mapping(
                        e ->
                            new Partition(
                                e.getKey().partition(),
                                e.getValue().earliest(),
                                e.getValue().latest(),
                                replicas.get(e.getKey()).stream()
                                    .map(Replica::new)
                                    .collect(Collectors.toUnmodifiableList())),
                        Collectors.toList())));

    var topicInfos =
        topics.entrySet().stream()
            .map(p -> new TopicInfo(p.getKey(), partitions.get(p.getKey()), p.getValue()))
            .collect(Collectors.toUnmodifiableList());

    if (topicNames.size() == 1 && topicInfos.size() == 1) return topicInfos.get(0);
    return new Topics(topicInfos);
  }

  static Map<String, String> remainingConfigs(PostRequest request) {
    var configs =
        new HashMap<>(
            request.raw().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    configs.remove(TOPIC_NAME_KEY);
    configs.remove(NUMBER_OF_PARTITIONS_KEY);
    configs.remove(NUMBER_OF_REPLICAS_KEY);
    return configs;
  }

  @Override
  public Response post(Channel channel) {
    admin
        .creator()
        .topic(channel.request().value(TOPIC_NAME_KEY))
        .numberOfPartitions(channel.request().getInt(NUMBER_OF_PARTITIONS_KEY).orElse(1))
        .numberOfReplicas(channel.request().getShort(NUMBER_OF_REPLICAS_KEY).orElse((short) 1))
        .configs(remainingConfigs(channel.request()))
        .create();
    if (admin.topicNames().contains(channel.request().value(TOPIC_NAME_KEY))) {
      try {
        // if the topic creation is synced, we return the details.
        return get(Set.of(channel.request().value(TOPIC_NAME_KEY)), ignored -> true);
      } catch (ExecutionRuntimeException executionRuntimeException) {
        if (UnknownTopicOrPartitionException.class
            != executionRuntimeException.getRootCause().getClass()) {
          throw executionRuntimeException;
        }
      }
    }
    // Otherwise, return only name
    return new TopicInfo(channel.request().value(TOPIC_NAME_KEY), List.of(), Map.of());
  }

  @Override
  public Response delete(Channel channel) {
    return channel
        .target()
        .map(
            topic -> {
              admin.deleteTopics(Set.of(topic));
              return Response.OK;
            })
        .orElse(Response.NOT_FOUND);
  }

  static class Topics implements Response {
    final Collection<TopicInfo> topics;

    private Topics(Collection<TopicInfo> topics) {
      this.topics = topics;
    }
  }

  static class TopicInfo implements Response {
    final String name;
    final List<Partition> partitions;
    final Map<String, String> configs;

    private TopicInfo(String name, List<Partition> partitions, Config configs) {
      this(
          name,
          partitions,
          StreamSupport.stream(configs.spliterator(), false)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private TopicInfo(String name, List<Partition> partitions, Map<String, String> configs) {
      this.name = name;
      this.partitions = partitions;
      this.configs = configs;
    }
  }

  static class Partition implements Response {
    final int id;
    final long earliest;
    final long latest;
    final List<Replica> replicas;

    Partition(int id, long earliest, long latest, List<Replica> replicas) {
      this.id = id;
      this.earliest = earliest;
      this.latest = latest;
      this.replicas = replicas;
    }
  }

  static class Replica implements Response {
    final int broker;
    final long lag;
    final long size;
    final boolean leader;
    final boolean inSync;
    final boolean isFuture;
    final String path;

    Replica(org.astraea.app.admin.Replica replica) {
      this(
          replica.nodeInfo().id(),
          replica.lag(),
          replica.size(),
          replica.isLeader(),
          replica.inSync(),
          replica.isFuture(),
          replica.dataFolder());
    }

    Replica(
        int broker,
        long lag,
        long size,
        boolean leader,
        boolean inSync,
        boolean isFuture,
        String path) {
      this.broker = broker;
      this.lag = lag;
      this.size = size;
      this.leader = leader;
      this.inSync = inSync;
      this.isFuture = isFuture;
      this.path = path;
    }
  }
}
