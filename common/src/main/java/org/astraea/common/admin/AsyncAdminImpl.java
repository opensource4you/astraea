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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.astraea.common.Utils;

class AsyncAdminImpl implements AsyncAdmin {

  private final org.apache.kafka.clients.admin.Admin kafkaAdmin;
  private final String clientId;
  private final List<?> pendingRequests;

  AsyncAdminImpl(Map<String, Object> props) {
    this(KafkaAdminClient.create(props));
  }

  AsyncAdminImpl(org.apache.kafka.clients.admin.Admin kafkaAdmin) {
    this.kafkaAdmin = kafkaAdmin;
    this.clientId = (String) Utils.member(kafkaAdmin, "clientId");
    this.pendingRequests =
        (ArrayList<?>) Utils.member(Utils.member(kafkaAdmin, "runnable"), "pendingCalls");
  }

  private static <T> CompletionStage<T> to(org.apache.kafka.common.KafkaFuture<T> kafkaFuture) {
    var f = new CompletableFuture<T>();
    kafkaFuture.whenComplete(
        (r, e) -> {
          if (e != null) f.completeExceptionally(e);
          else f.completeAsync(() -> r);
        });
    return f;
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public int pendingRequests() {
    return pendingRequests.size();
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
                                            // the old kafka does not support to fetch max timestamp
                                            .handle(
                                                (r, e) ->
                                                    e == null
                                                        ? r
                                                        : Map
                                                            .<String,
                                                                ListOffsetsResult
                                                                    .ListOffsetsResultInfo>
                                                                of())
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
  public CompletionStage<Set<String>> consumerGroupIds() {
    return to(kafkaAdmin.listConsumerGroups().all())
        .thenApply(
            gs ->
                gs.stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toUnmodifiableSet()));
  }

  @Override
  public CompletionStage<List<ConsumerGroup>> consumerGroups(Set<String> consumerGroupIds) {
    return to(kafkaAdmin.describeConsumerGroups(consumerGroupIds).all())
        .thenCombine(
            Utils.sequence(
                    consumerGroupIds.stream()
                        .map(
                            id ->
                                kafkaAdmin
                                    .listConsumerGroupOffsets(id)
                                    .partitionsToOffsetAndMetadata()
                                    .thenApply(of -> Map.entry(id, of)))
                        .map(f -> to(f).toCompletableFuture())
                        .collect(Collectors.toUnmodifiableList()))
                .thenApply(
                    s ->
                        s.stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
            (consumerGroupDescriptions, consumerGroupMetadata) ->
                consumerGroupIds.stream()
                    .map(
                        groupId ->
                            new ConsumerGroup(
                                groupId,
                                NodeInfo.of(consumerGroupDescriptions.get(groupId).coordinator()),
                                consumerGroupMetadata.get(groupId).entrySet().stream()
                                    .collect(
                                        Collectors.toUnmodifiableMap(
                                            tp -> TopicPartition.from(tp.getKey()),
                                            offset -> offset.getValue().offset())),
                                consumerGroupDescriptions.get(groupId).members().stream()
                                    .collect(
                                        Collectors.toUnmodifiableMap(
                                            member ->
                                                new Member(
                                                    groupId,
                                                    member.consumerId(),
                                                    member.groupInstanceId(),
                                                    member.clientId(),
                                                    member.host()),
                                            member ->
                                                member.assignment().topicPartitions().stream()
                                                    .map(TopicPartition::from)
                                                    .collect(Collectors.toSet())))))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<ProducerState>> producerStates(Set<TopicPartition> partitions) {
    return to(kafkaAdmin
            .describeProducers(
                partitions.stream()
                    .map(TopicPartition::to)
                    .collect(Collectors.toUnmodifiableList()))
            .all())
        .thenApply(
            ps ->
                ps.entrySet().stream()
                    .flatMap(
                        e ->
                            e.getValue().activeProducers().stream()
                                .map(s -> ProducerState.of(e.getKey(), s)))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<AddingReplica>> addingReplicas(Set<String> topics) {

    return topicPartitions(topics)
        .thenApply(ps -> ps.stream().map(TopicPartition::to).collect(Collectors.toSet()))
        .thenCompose(ps -> to(kafkaAdmin.listPartitionReassignments(ps).reassignments()))
        .thenApply(
            pr ->
                pr.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().addingReplicas().stream()
                                .map(
                                    id ->
                                        new org.apache.kafka.common.TopicPartitionReplica(
                                            entry.getKey().topic(),
                                            entry.getKey().partition(),
                                            id)))
                    .collect(Collectors.toList()))
        .thenCombine(
            logDirs(),
            (adding, dirs) -> {
              Function<TopicPartition, Long> findMaxSize =
                  tp ->
                      dirs.values().stream()
                          .flatMap(e -> e.entrySet().stream())
                          .filter(e -> e.getKey().equals(tp))
                          .flatMap(e -> e.getValue().values().stream().map(ReplicaInfo::size))
                          .mapToLong(v -> v)
                          .max()
                          .orElse(0);
              return adding.stream()
                  .filter(
                      r ->
                          dirs.getOrDefault(r.brokerId(), Map.of())
                              .containsKey(TopicPartition.of(r.topic(), r.partition())))
                  .flatMap(
                      r ->
                          dirs
                              .get(r.brokerId())
                              .get(TopicPartition.of(r.topic(), r.partition()))
                              .entrySet()
                              .stream()
                              .map(
                                  entry ->
                                      AddingReplica.of(
                                          r.topic(),
                                          r.partition(),
                                          r.brokerId(),
                                          entry.getKey(),
                                          entry.getValue().size(),
                                          findMaxSize.apply(
                                              TopicPartition.of(r.topic(), r.partition())))))
                  .collect(Collectors.toList());
            });
  }

  @Override
  public CompletionStage<Set<String>> transactionIds() {
    return to(kafkaAdmin.listTransactions().all())
        .thenApply(
            t ->
                t.stream()
                    .map(TransactionListing::transactionalId)
                    .collect(Collectors.toUnmodifiableSet()));
  }

  @Override
  public CompletionStage<List<Transaction>> transactions(Set<String> transactionIds) {
    return to(kafkaAdmin.describeTransactions(transactionIds).all())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    .map(e -> Transaction.of(e.getKey(), e.getValue()))
                    .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public CompletionStage<List<Replica>> replicas(Set<String> topics) {
    // pre-group folders by (broker -> topic partition) to speedup seek
    return logDirs()
        .thenCombine(
            to(kafkaAdmin.describeTopics(topics).allTopicNames()),
            (logDirs, topicDesc) ->
                topicDesc.entrySet().stream()
                    .flatMap(
                        topicDes ->
                            topicDes.getValue().partitions().stream()
                                .flatMap(
                                    tpInfo ->
                                        tpInfo.replicas().stream()
                                            .flatMap(
                                                node -> {
                                                  // kafka admin#describeLogDirs does not return
                                                  // offline
                                                  // node,when the node is not online,all
                                                  // TopicPartition
                                                  // return an empty dataFolder and a
                                                  // fake replicaInfo, and determine whether the
                                                  // node is
                                                  // online by whether the dataFolder is "".
                                                  var pathAndReplicas =
                                                      logDirs
                                                          .getOrDefault(node.id(), Map.of())
                                                          .getOrDefault(
                                                              TopicPartition.of(
                                                                  topicDes.getKey(),
                                                                  tpInfo.partition()),
                                                              Map.of(
                                                                  "",
                                                                  new org.apache.kafka.clients.admin
                                                                      .ReplicaInfo(
                                                                      -1L, -1L, false)));
                                                  return pathAndReplicas.entrySet().stream()
                                                      .map(
                                                          pathAndReplica ->
                                                              Replica.of(
                                                                  topicDes.getKey(),
                                                                  tpInfo.partition(),
                                                                  NodeInfo.of(node),
                                                                  pathAndReplica
                                                                      .getValue()
                                                                      .offsetLag(),
                                                                  pathAndReplica.getValue().size(),
                                                                  tpInfo.leader() != null
                                                                      && !tpInfo.leader().isEmpty()
                                                                      && tpInfo.leader().id()
                                                                          == node.id(),
                                                                  tpInfo.isr().contains(node),
                                                                  pathAndReplica
                                                                      .getValue()
                                                                      .isFuture(),
                                                                  node.isEmpty()
                                                                      || pathAndReplica
                                                                          .getKey()
                                                                          .equals(""),
                                                                  // The first replica in the return
                                                                  // result is the preferred leader.
                                                                  // This only works with Kafka
                                                                  // broker version after
                                                                  // 0.11. Version before 0.11
                                                                  // returns the replicas in
                                                                  // unspecified order.
                                                                  tpInfo.replicas().get(0).id()
                                                                      == node.id(),
                                                                  // empty data folder means this
                                                                  // replica is offline
                                                                  pathAndReplica.getKey().isEmpty()
                                                                      ? null
                                                                      : pathAndReplica.getKey()));
                                                })))
                    .collect(Collectors.toList()));
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
  public ReplicaMigrator migrator() {
    return new ReplicaMigrator() {
      private final List<CompletableFuture<Set<TopicPartition>>> partitions =
          Collections.synchronizedList(new ArrayList<>());

      @Override
      public ReplicaMigrator topic(String topic) {
        partitions.add(topicPartitions(Set.of(topic)).toCompletableFuture());
        return this;
      }

      @Override
      public ReplicaMigrator partition(String topic, int partition) {
        partitions.add(
            CompletableFuture.completedFuture(Set.of(TopicPartition.of(topic, partition))));
        return this;
      }

      @Override
      public ReplicaMigrator broker(int broker) {
        partitions.add(topicPartitions(broker).toCompletableFuture());
        return this;
      }

      @Override
      public ReplicaMigrator topicOfBroker(int broker, String topic) {
        partitions.add(
            topicPartitions(broker)
                .toCompletableFuture()
                .thenApply(
                    ps ->
                        ps.stream()
                            .filter(p -> p.topic().equals(topic))
                            .collect(Collectors.toUnmodifiableSet())));
        return this;
      }

      @Override
      public CompletionStage<Void> moveTo(List<Integer> brokers) {
        return Utils.sequence(partitions)
            .thenApply(ss -> ss.stream().flatMap(Collection::stream).collect(Collectors.toList()))
            .thenCompose(
                partitions ->
                    to(
                        kafkaAdmin
                            .alterPartitionReassignments(
                                partitions.stream()
                                    .collect(
                                        Collectors.toMap(
                                            p ->
                                                new org.apache.kafka.common.TopicPartition(
                                                    p.topic(), p.partition()),
                                            ignore ->
                                                Optional.of(
                                                    new NewPartitionReassignment(brokers)))))
                            .all()));
      }

      @Override
      public CompletionStage<Void> moveTo(Map<Integer, String> brokerFolders) {
        var f = new CompletableFuture<Void>();
        var ps =
            Utils.sequence(partitions)
                .thenApply(
                    ss -> ss.stream().flatMap(Collection::stream).collect(Collectors.toList()))
                .thenCompose(
                    tps ->
                        partitions(
                                tps.stream()
                                    .map(TopicPartition::topic)
                                    .collect(Collectors.toUnmodifiableSet()))
                            .thenApply(
                                partitions ->
                                    partitions.stream()
                                        .filter(
                                            p ->
                                                tps.contains(
                                                    TopicPartition.of(p.topic(), p.partition())))
                                        .collect(Collectors.toList())));

        ps.whenComplete(
            (partitions, e) -> {
              if (e != null) {
                f.completeExceptionally(e);
                return;
              }
              for (var p : partitions) {
                var currentBrokerIds =
                    p.replicas().stream().map(NodeInfo::id).collect(Collectors.toList());
                if (!brokerFolders.keySet().containsAll(currentBrokerIds)) {
                  f.completeExceptionally(
                      new IllegalStateException(
                          p.topic()
                              + " is located at "
                              + currentBrokerIds
                              + " which is not matched to expected brokers: "
                              + brokerFolders.keySet()
                              + ". Please use moveTo(List<Integer>) first"));
                  return;
                }
              }
              for (var p : partitions) {
                var payload =
                    brokerFolders.entrySet().stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                entry ->
                                    new org.apache.kafka.common.TopicPartitionReplica(
                                        p.topic(), p.partition(), entry.getKey()),
                                Map.Entry::getValue));
                to(kafkaAdmin.alterReplicaLogDirs(payload).all())
                    .whenComplete(
                        (ignored, e2) -> {
                          if (e2 != null) f.completeExceptionally(e2);
                          else f.complete(null);
                        });
              }
            });
        return f;
      }
    };
  }

  @Override
  public CompletionStage<Void> preferredLeaderElection(TopicPartition topicPartition) {
    var f = new CompletableFuture<Void>();

    to(kafkaAdmin
            .electLeaders(ElectionType.PREFERRED, Set.of(TopicPartition.to(topicPartition)))
            .all())
        .whenComplete(
            (ignored, e) -> {
              if (e == null) f.complete(null);
              // Swallow the ElectionNotNeededException.
              // This error occurred if the preferred leader of the given topic/partition is already
              // the
              // leader. It is ok to swallow the exception since the preferred leader be the actual
              // leader. That is what the caller wants to be.
              else if (e instanceof ExecutionException
                  && e.getCause() instanceof ElectionNotNeededException) f.complete(null);
              else if (e instanceof ElectionNotNeededException) f.complete(null);
              else f.completeExceptionally(e);
            });
    return f;
  }

  @Override
  public CompletionStage<Void> addPartitions(String topic, int total) {
    return to(kafkaAdmin.createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
  }

  @Override
  public void close() {
    kafkaAdmin.close();
  }

  private CompletionStage<
          Map<
              Integer,
              Map<TopicPartition, Map<String, org.apache.kafka.clients.admin.ReplicaInfo>>>>
      logDirs() {
    return nodeInfos()
        .thenApply(
            nodeInfos ->
                nodeInfos.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()))
        .thenCompose(ids -> to(kafkaAdmin.describeLogDirs(ids).allDescriptions()))
        .thenApply(
            ds ->
                ds.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            pathAndDesc ->
                                pathAndDesc.getValue().entrySet().stream()
                                    .flatMap(
                                        e ->
                                            e.getValue().replicaInfos().entrySet().stream()
                                                .map(
                                                    tr ->
                                                        Map.entry(
                                                            TopicPartition.from(tr.getKey()),
                                                            Map.entry(e.getKey(), tr.getValue()))))
                                    .collect(Collectors.groupingBy(Map.Entry::getKey))
                                    .entrySet()
                                    .stream()
                                    .collect(
                                        Collectors.toMap(
                                            Map.Entry::getKey,
                                            e ->
                                                e.getValue().stream()
                                                    .map(Map.Entry::getValue)
                                                    .collect(
                                                        Collectors.toMap(
                                                            Map.Entry::getKey,
                                                            Map.Entry::getValue)))))));
  }
}
