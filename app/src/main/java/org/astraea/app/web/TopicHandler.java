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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.common.FutureUtils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.scenario.Scenario;

class TopicHandler implements Handler {

  static final String TOPICS_KEY = "topics";

  static final String TOPIC_NAME_KEY = "name";
  static final String NUMBER_OF_PARTITIONS_KEY = "partitions";
  static final String NUMBER_OF_REPLICAS_KEY = "replicas";
  static final String PARTITION_KEY = "partition";
  static final String LIST_INTERNAL = "listInternal";
  static final String PROBABILITY_INTERNAL = "probability";

  private final AsyncAdmin admin;

  TopicHandler(AsyncAdmin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    return admin
        .topicNames(
            Optional.ofNullable(channel.queries().get(LIST_INTERNAL))
                .map(Boolean::parseBoolean)
                .orElse(true))
        .thenApply(
            topics -> {
              var availableTopics =
                  channel.target().map(Set::of).orElse(topics).stream()
                      .filter(topics::contains)
                      .collect(Collectors.toSet());
              if (availableTopics.isEmpty() && channel.target().isPresent())
                throw new NoSuchElementException(
                    "topic: " + channel.target().get() + " is nonexistent");
              return availableTopics;
            })
        .thenCompose(
            topics ->
                get(
                    topics,
                    partition ->
                        !channel.queries().containsKey(PARTITION_KEY)
                            || partition == Integer.parseInt(channel.queries().get(PARTITION_KEY))))
        .thenApply(topics -> topics.topics.size() == 1 ? topics.topics.get(0) : topics);
  }

  private CompletionStage<Topics> get(
      Set<String> topicNames, Predicate<Integer> partitionPredicate) {
    return FutureUtils.combine(
        admin.replicas(topicNames),
        admin.partitions(topicNames),
        admin.topics(topicNames),
        admin.consumerGroupIds().thenCompose(admin::consumerGroups),
        (replicas, partitions, topics, groups) -> {
          var ps =
              partitions.stream()
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
                                      p.maxTimestamp().orElse(null),
                                      replicas.stream()
                                          .filter(replica -> replica.topic().equals(p.topic()))
                                          .filter(replica -> replica.partition() == p.partition())
                                          .map(Replica::new)
                                          .collect(Collectors.toUnmodifiableList())),
                              Collectors.toList())));
          // topic name -> group ids
          var gs =
              groups.stream()
                  .flatMap(
                      g ->
                          g.assignment().entrySet().stream()
                              .flatMap(
                                  m ->
                                      m.getValue().stream()
                                          .map(TopicPartition::topic)
                                          .distinct()
                                          .map(t -> Map.entry(t, g.groupId()))))
                  .collect(Collectors.groupingBy(Map.Entry::getKey))
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey,
                          e ->
                              e.getValue().stream()
                                  .map(Map.Entry::getValue)
                                  .collect(Collectors.toSet())));
          return new Topics(
              topics.stream()
                  .map(
                      topic ->
                          new TopicInfo(
                              topic.name(),
                              gs.getOrDefault(topic.name(), Set.of()),
                              ps.get(topic.name()),
                              topic.config().raw()))
                  .collect(Collectors.toUnmodifiableList()));
        });
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
  public CompletionStage<Topics> post(Channel channel) {
    var requests = channel.request().requests(TOPICS_KEY);
    var topicNames =
        requests.stream().map(r -> r.value(TOPIC_NAME_KEY)).collect(Collectors.toSet());
    if (topicNames.size() != requests.size())
      throw new IllegalArgumentException("duplicate topic name: " + topicNames);
    return FutureUtils.sequence(
            requests.stream()
                .map(
                    request -> {
                      var topicName = request.value(TOPIC_NAME_KEY);
                      var numberOfPartitions = request.getInt(NUMBER_OF_PARTITIONS_KEY).orElse(1);
                      var numberOfReplicas =
                          request.getShort(NUMBER_OF_REPLICAS_KEY).orElse((short) 1);
                      if (request.has(PROBABILITY_INTERNAL))
                        return Scenario.build(request.doubleValue(PROBABILITY_INTERNAL))
                            .topicName(topicName)
                            .numberOfPartitions(numberOfPartitions)
                            .numberOfReplicas(numberOfReplicas)
                            .build()
                            .apply(admin)
                            .thenApply(ignored -> null)
                            .toCompletableFuture();
                      return admin
                          .creator()
                          .topic(request.value(TOPIC_NAME_KEY))
                          .numberOfPartitions(request.getInt(NUMBER_OF_PARTITIONS_KEY).orElse(1))
                          .numberOfReplicas(
                              request.getShort(NUMBER_OF_REPLICAS_KEY).orElse((short) 1))
                          .configs(remainingConfigs(request))
                          .run()
                          .thenApply(ignored -> null)
                          .toCompletableFuture();
                    })
                .collect(Collectors.toList()))
        .thenCompose(ignored -> get(topicNames, id -> true))
        .exceptionally(
            ignored ->
                new Topics(
                    topicNames.stream()
                        .map(t -> new TopicInfo(t, Set.of(), List.of(), Map.of()))
                        .collect(Collectors.toUnmodifiableList())));
  }

  @Override
  public CompletionStage<Response> delete(Channel channel) {
    return channel
        .target()
        .map(topic -> admin.deleteTopics(Set.of(topic)).thenApply(ignored -> Response.OK))
        .orElse(CompletableFuture.completedStage(Response.NOT_FOUND));
  }

  static class Topics implements Response {
    final List<TopicInfo> topics;

    private Topics(List<TopicInfo> topics) {
      this.topics = topics;
    }
  }

  static class TopicInfo implements Response {
    final String name;

    final Set<String> activeGroupIds;
    final List<Partition> partitions;
    final Map<String, String> configs;

    private TopicInfo(
        String name,
        Set<String> groupIds,
        List<Partition> partitions,
        Map<String, String> configs) {
      this.name = name;
      this.activeGroupIds = groupIds;
      this.partitions = partitions;
      this.configs = configs;
    }
  }

  static class Partition implements Response {
    final int id;
    final long earliest;
    final long latest;
    final List<Replica> replicas;

    // previous kafka does not support to query max timestamp
    final Long maxTimestamp;

    Partition(int id, long earliest, long latest, Long maxTimestamp, List<Replica> replicas) {
      this.id = id;
      this.earliest = earliest;
      this.latest = latest;
      this.replicas = replicas;
      this.maxTimestamp = maxTimestamp;
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
          replica.path());
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
