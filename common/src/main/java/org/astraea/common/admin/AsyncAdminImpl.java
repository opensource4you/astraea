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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.astraea.common.Utils;

class AsyncAdminImpl implements AsyncAdmin {

  private final org.apache.kafka.clients.admin.Admin kafkaAdmin;

  AsyncAdminImpl(org.apache.kafka.clients.admin.Admin kafkaAdmin) {
    this.kafkaAdmin = kafkaAdmin;
  }

  private static <T> CompletionStage<T> to(org.apache.kafka.common.KafkaFuture<T> kafkaFuture) {
    var f = new CompletableFuture<T>();
    kafkaFuture.whenComplete(
        (r, e) -> {
          if (e != null) f.completeExceptionally(e);
          else f.complete(r);
        });
    return f;
  }

  @Override
  public CompletionStage<Set<String>> topicNames(boolean listInternal) {
    return to(kafkaAdmin
            .listTopics(new ListTopicsOptions().listInternal(listInternal))
            .namesToListings())
        .thenApply(Map::keySet);
  }

  @Override
  public CompletionStage<List<Topic>> topics(Set<String> names) {
    return to(kafkaAdmin
            .describeConfigs(
                names.stream()
                    .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                    .collect(Collectors.toList()))
            .all())
        .thenApply(
            r ->
                r.entrySet().stream()
                    .map(entry -> Topic.of(entry.getKey().name(), entry.getValue()))
                    .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public CompletionStage<Void> deleteTopics(Set<String> topics) {
    return to(kafkaAdmin.deleteTopics(topics).all());
  }

  @Override
  public CompletionStage<Set<TopicPartition>> topicPartitions(Set<String> topics) {
    return to(kafkaAdmin.describeTopics(topics).all())
        .thenApply(
            r ->
                r.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().partitions().stream()
                                .map(p -> TopicPartition.of(entry.getKey(), p.partition())))
                    .collect(Collectors.toSet()));
  }

  @Override
  public CompletionStage<Set<TopicPartition>> topicPartitions(int brokerId) {
    return topicNames(true)
        .thenCompose(topics -> to(kafkaAdmin.describeTopics(topics).all()))
        .thenApply(
            r ->
                r.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().partitions().stream()
                                .filter(
                                    p -> p.replicas().stream().anyMatch(n -> n.id() == brokerId))
                                .map(p -> TopicPartition.of(entry.getKey(), p.partition())))
                    .collect(Collectors.toSet()));
  }

  @Override
  public CompletionStage<List<Partition>> partitions(Set<String> topics) {
    return to(kafkaAdmin.describeTopics(topics).all())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    .flatMap(
                        e ->
                            e.getValue().partitions().stream()
                                .map(
                                    tp ->
                                        Map.entry(
                                            new org.apache.kafka.common.TopicPartition(
                                                e.getKey(), tp.partition()),
                                            tp)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .thenCompose(
            partitions ->
                to(kafkaAdmin
                        .listOffsets(
                            partitions.keySet().stream()
                                .collect(
                                    Collectors.toMap(
                                        Function.identity(), e -> new OffsetSpec.EarliestSpec())))
                        .all())
                    .thenCompose(
                        earliest ->
                            to(kafkaAdmin
                                    .listOffsets(
                                        partitions.keySet().stream()
                                            .collect(
                                                Collectors.toMap(
                                                    Function.identity(),
                                                    e -> new OffsetSpec.LatestSpec())))
                                    .all())
                                .thenCompose(
                                    latest ->
                                        to(kafkaAdmin
                                                .listOffsets(
                                                    partitions.keySet().stream()
                                                        .collect(
                                                            Collectors.toMap(
                                                                Function.identity(),
                                                                e ->
                                                                    new OffsetSpec
                                                                        .MaxTimestampSpec())))
                                                .all())
                                            .thenApply(
                                                maxTimestamp ->
                                                    partitions.entrySet().stream()
                                                        .map(
                                                            entry ->
                                                                Partition.of(
                                                                    entry.getKey().topic(),
                                                                    entry.getValue(),
                                                                    Optional.ofNullable(
                                                                        earliest.get(
                                                                            entry.getKey())),
                                                                    Optional.ofNullable(
                                                                        latest.get(entry.getKey())),
                                                                    Optional.ofNullable(
                                                                        maxTimestamp.get(
                                                                            entry.getKey()))))
                                                        .collect(Collectors.toList())))));
  }

  @Override
  public CompletionStage<Set<NodeInfo>> nodeInfos() {
    return to(kafkaAdmin.describeCluster().nodes())
        .thenApply(
            nodes -> nodes.stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableSet()));
  }

  @Override
  public CompletionStage<List<Broker>> brokers() {
    var cluster = kafkaAdmin.describeCluster();
    return to(cluster.controller())
        .thenCompose(
            controller ->
                to(cluster.nodes())
                    .thenCompose(
                        nodes ->
                            to(kafkaAdmin
                                    .describeLogDirs(
                                        nodes.stream().map(Node::id).collect(Collectors.toList()))
                                    .all())
                                .thenCompose(
                                    logDirs ->
                                        to(kafkaAdmin
                                                .describeConfigs(
                                                    nodes.stream()
                                                        .map(
                                                            n ->
                                                                new ConfigResource(
                                                                    ConfigResource.Type.BROKER,
                                                                    String.valueOf(n.id())))
                                                        .collect(Collectors.toList()))
                                                .all())
                                            .thenApply(
                                                configs ->
                                                    configs.entrySet().stream()
                                                        .collect(
                                                            Collectors.toMap(
                                                                e ->
                                                                    Integer.valueOf(
                                                                        e.getKey().name()),
                                                                Map.Entry::getValue)))
                                            .thenCompose(
                                                configs ->
                                                    topicNames(true)
                                                        .thenCompose(
                                                            names ->
                                                                to(
                                                                    kafkaAdmin
                                                                        .describeTopics(names)
                                                                        .all()))
                                                        .thenApply(
                                                            topics ->
                                                                nodes.stream()
                                                                    .map(
                                                                        node ->
                                                                            Broker.of(
                                                                                node.id()
                                                                                    == controller
                                                                                        .id(),
                                                                                node,
                                                                                configs.get(
                                                                                    node.id()),
                                                                                logDirs.get(
                                                                                    node.id()),
                                                                                topics.values()))
                                                                    .collect(
                                                                        Collectors.toList()))))));
  }

  @Override
  public TopicCreator creator() {
    return new TopicCreator() {
      private String topic;
      private int numberOfPartitions = 1;
      private short numberOfReplicas = 1;
      private Map<String, String> configs = Map.of();

      @Override
      public TopicCreator topic(String topic) {
        this.topic = topic;
        return this;
      }

      @Override
      public TopicCreator numberOfPartitions(int numberOfPartitions) {
        this.numberOfPartitions = numberOfPartitions;
        return this;
      }

      @Override
      public TopicCreator numberOfReplicas(short numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        return this;
      }

      @Override
      public TopicCreator configs(Map<String, String> configs) {
        this.configs = configs;
        return this;
      }

      @Override
      public CompletionStage<Boolean> run() {
        Utils.requireNonEmpty(topic);
        Utils.requirePositive(numberOfPartitions);
        Utils.requirePositive(numberOfReplicas);
        Objects.requireNonNull(configs);

        return topicNames(true)
            .thenCompose(
                names -> {
                  if (!names.contains(topic))
                    return to(kafkaAdmin
                            .createTopics(
                                List.of(
                                    new NewTopic(topic, numberOfPartitions, numberOfReplicas)
                                        .configs(configs)))
                            .all())
                        .thenApply(r -> true);
                  return topics(Set.of(topic))
                      .thenCombine(
                          partitions(Set.of(topic)),
                          (ts, partitions) -> {
                            if (partitions.size() != numberOfPartitions)
                              throw new IllegalArgumentException(
                                  topic
                                      + " is existent but its partitions: "
                                      + partitions.size()
                                      + " is not equal to expected: "
                                      + numberOfPartitions);
                            partitions.forEach(
                                p -> {
                                  if (p.replicas().size() != numberOfReplicas)
                                    throw new IllegalArgumentException(
                                        topic
                                            + " is existent but its replicas: "
                                            + p.replicas().size()
                                            + " is not equal to expected: "
                                            + numberOfReplicas);
                                });

                            ts.stream()
                                .filter(t -> t.name().equals(topic))
                                .map(Topic::config)
                                .forEach(
                                    existentConfigs ->
                                        configs.forEach(
                                            (k, v) -> {
                                              if (existentConfigs
                                                  .value(k)
                                                  .filter(actual -> actual.equals(v))
                                                  .isEmpty())
                                                throw new IllegalArgumentException(
                                                    topic
                                                        + " is existent but its config: <"
                                                        + k
                                                        + ", "
                                                        + existentConfigs.value(k)
                                                        + "> is not equal to expected: "
                                                        + k
                                                        + ", "
                                                        + v);
                                            }));

                            // there is equal topic
                            return false;
                          });
                });
      }
    };
  }

  @Override
  public void close() {
    kafkaAdmin.close();
  }
}
