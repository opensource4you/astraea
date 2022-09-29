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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.astraea.app.scenario.Scenario;
import org.astraea.common.ExecutionRuntimeException;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Config;

class TopicHandler implements Handler {

  static final String TOPICS_KEY = "topics";

  static final String TOPIC_NAME_KEY = "name";
  static final String NUMBER_OF_PARTITIONS_KEY = "partitions";
  static final String NUMBER_OF_REPLICAS_KEY = "replicas";
  static final String PARTITION_KEY = "partition";
  static final String LIST_INTERNAL = "listInternal";
  static final String PROBABILITY_INTERNAL = "probability";

  private final Admin admin;

  TopicHandler(Admin admin) {
    this.admin = admin;
  }

  Set<String> topicNames(Optional<String> target, boolean listInternal) {
    return Handler.compare(admin.topicNames(listInternal), target);
  }

  @Override
  public Response get(Channel channel) {
    var topicNames =
        topicNames(
            channel.target(),
            Optional.ofNullable(channel.queries().get(LIST_INTERNAL))
                .map(Boolean::parseBoolean)
                .orElse(true));
    var topics =
        get(
            topicNames,
            partition ->
                !channel.queries().containsKey(PARTITION_KEY)
                    || partition == Integer.parseInt(channel.queries().get(PARTITION_KEY)));
    if (topicNames.size() == 1) return topics.topics.get(0);
    return topics;
  }

  private Topics get(Set<String> topicNames, Predicate<Integer> partitionPredicate) {
    var replicas = admin.newReplicas(topicNames);
    var partitions =
        admin.partitions(topicNames).stream()
            .filter(p -> partitionPredicate.test(p.partition()))
            .collect(
                Collectors.groupingBy(
                    org.astraea.common.admin.Partition::topic,
                    Collectors.mapping(
                        p ->
                            new Partition(
                                p.partition(),
                                p.earliestOffset(),
                                p.latestOffset(),
                                replicas.stream()
                                    .filter(replica -> replica.topic().equals(p.topic()))
                                    .filter(replica -> replica.partition() == p.partition())
                                    .map(Replica::new)
                                    .collect(Collectors.toUnmodifiableList())),
                        Collectors.toList())));

    var topicInfos =
        admin.topics(topicNames).stream()
            .map(topic -> new TopicInfo(topic.name(), partitions.get(topic.name()), topic.config()))
            .collect(Collectors.toUnmodifiableList());
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
  public Topics post(Channel channel) {
    var requests = channel.request().requests(TOPICS_KEY);
    var topicNames =
        requests.stream().map(r -> r.value(TOPIC_NAME_KEY)).collect(Collectors.toSet());
    if (topicNames.size() != requests.size())
      throw new IllegalArgumentException("duplicate topic name: " + topicNames);
    requests.forEach(
        request -> {
          var topicName = request.value(TOPIC_NAME_KEY);
          var numberOfPartitions = request.getInt(NUMBER_OF_PARTITIONS_KEY).orElse(1);
          var numberOfReplicas = request.getShort(NUMBER_OF_REPLICAS_KEY).orElse((short) 1);
          if (request.has(PROBABILITY_INTERNAL)) {
            Scenario.build(request.doubleValue(PROBABILITY_INTERNAL))
                .topicName(topicName)
                .numberOfPartitions(numberOfPartitions)
                .numberOfReplicas(numberOfReplicas)
                .build()
                .apply(admin);
          } else {
            admin
                .creator()
                .topic(request.value(TOPIC_NAME_KEY))
                .numberOfPartitions(request.getInt(NUMBER_OF_PARTITIONS_KEY).orElse(1))
                .numberOfReplicas(request.getShort(NUMBER_OF_REPLICAS_KEY).orElse((short) 1))
                .configs(remainingConfigs(request))
                .create();
          }
        });

    try {
      // if the topic creation is synced, we return the details.
      return get(topicNames, ignored -> true);
    } catch (ExecutionRuntimeException executionRuntimeException) {
      if (UnknownTopicOrPartitionException.class
          != executionRuntimeException.getRootCause().getClass()) {
        throw executionRuntimeException;
      }
    }
    // Otherwise, return only name
    return new Topics(
        topicNames.stream()
            .map(t -> new TopicInfo(t, List.of(), Map.of()))
            .collect(Collectors.toUnmodifiableList()));
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
    final List<TopicInfo> topics;

    private Topics(List<TopicInfo> topics) {
      this.topics = topics;
    }
  }

  static class TopicInfo implements Response {
    final String name;
    final List<Partition> partitions;
    final Map<String, String> configs;

    private TopicInfo(String name, List<Partition> partitions, Config configs) {
      this(name, partitions, configs.raw());
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

    Replica(org.astraea.common.admin.Replica replica) {
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
