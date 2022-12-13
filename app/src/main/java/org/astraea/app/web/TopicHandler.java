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
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.json.TypeRef;

class TopicHandler implements Handler {

  static final String PARTITION_KEY = "partition";
  static final String LIST_INTERNAL = "listInternal";
  static final String POLL_RECORD_TIMEOUT = "poll_timeout";

  private final Admin admin;

  TopicHandler(Admin admin) {
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
                    Optional.ofNullable(channel.queries().get(POLL_RECORD_TIMEOUT))
                        .map(Utils::toDuration)
                        .orElse(null),
                    partition ->
                        !channel.queries().containsKey(PARTITION_KEY)
                            || partition == Integer.parseInt(channel.queries().get(PARTITION_KEY))))
        .thenApply(topics -> topics.topics.size() == 1 ? topics.topics.get(0) : topics);
  }

  private CompletionStage<Topics> get(
      Set<String> topicNames, Duration pollTimeout, Predicate<Integer> partitionPredicate) {
    var timestampOfRecords =
        pollTimeout == null
            ? CompletableFuture.completedStage(Map.<TopicPartition, Long>of())
            : admin
                .topicPartitions(topicNames)
                .thenCompose(
                    tps ->
                        admin.timestampOfLatestRecords(
                            tps.stream()
                                .filter(p -> partitionPredicate.test(p.partition()))
                                .collect(Collectors.toSet()),
                            pollTimeout));

    return FutureUtils.combine(
        admin.clusterInfo(topicNames),
        admin.partitions(topicNames),
        admin.topics(topicNames),
        admin.consumerGroupIds().thenCompose(admin::consumerGroups),
        timestampOfRecords,
        (clusterInfo, partitions, topics, groups, recordTimestamp) -> {
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
                                      recordTimestamp.get(p.topicPartition()),
                                      clusterInfo
                                          .replicaStream()
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

  @Override
  public CompletionStage<Topics> post(Channel channel) {
    var postRequest = channel.request(TypeRef.of(TopicPostRequest.class));

    var topicNames = postRequest.topics.stream().map(x -> x.name).collect(Collectors.toSet());
    if (topicNames.size() != postRequest.topics.size())
      throw new IllegalArgumentException("duplicate topic name: " + topicNames);
    return FutureUtils.sequence(
            postRequest.topics.stream()
                .map(
                    topic -> {
                      var topicName = topic.name;
                      var numberOfPartitions = topic.partitions;
                      var numberOfReplicas = topic.replicas;

                      if (topic.probability.isPresent()) {
                        return Scenario.builder()
                            .topicName(topicName)
                            .numberOfPartitions(numberOfPartitions)
                            .numberOfReplicas(numberOfReplicas)
                            .binomialProbability(topic.probability.get())
                            .build()
                            .apply(admin)
                            .thenApply(ignored -> null)
                            .toCompletableFuture();
                      }

                      return admin
                          .creator()
                          .topic(topicName)
                          .numberOfPartitions(numberOfPartitions)
                          .numberOfReplicas(numberOfReplicas)
                          .configs(topic.configs)
                          .run()
                          .thenApply(ignored -> null)
                          .toCompletableFuture();
                    })
                .collect(Collectors.toList()))
        .thenCompose(ignored -> get(topicNames, null, id -> true))
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

  static class TopicPostRequest implements Request {
    List<Topic> topics = List.of();
  }

  static class Topic implements Request {
    String name;
    int partitions = 1;
    short replicas = 1;
    Optional<Double> probability = Optional.empty();
    Map<String, String> configs = Map.of();
  }

  static class Topics implements Response {
    final List<TopicInfo> topics;

    private Topics() {
      this.topics = List.of();
    }

    private Topics(List<TopicInfo> topics) {
      this.topics = topics;
    }
  }

  static class TopicInfo implements Response {
    final String name;

    final Set<String> activeGroupIds;
    final List<Partition> partitions;
    final Map<String, String> configs;

    private TopicInfo() {
      name = "";
      activeGroupIds = Set.of();
      partitions = List.of();
      configs = Map.of();
    }

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

    // optional field
    final Long timestampOfLatestRecord;

    private Partition() {
      id = -1;
      earliest = 0;
      latest = 0;
      replicas = List.of();
      maxTimestamp = 0L;
      timestampOfLatestRecord = 0L;
    }

    Partition(
        int id,
        long earliest,
        long latest,
        Long maxTimestamp,
        Long timestampOfLatestRecord,
        List<Replica> replicas) {
      this.id = id;
      this.earliest = earliest;
      this.latest = latest;
      this.replicas = replicas;
      this.maxTimestamp = maxTimestamp;
      this.timestampOfLatestRecord = timestampOfLatestRecord;
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

    private Replica() {
      broker = -1;
      lag = 0;
      size = 0;
      leader = false;
      inSync = false;
      isFuture = false;
      path = "";
    }

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
