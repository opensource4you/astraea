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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;

class AsyncAdminImpl implements AsyncAdmin {

  private final org.apache.kafka.clients.admin.Admin kafkaAdmin;
  private final String clientId;
  private final List<?> pendingRequests;

  AsyncAdminImpl(Map<String, String> props) {
    this(
        KafkaAdminClient.create(
            props.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
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
        .thenApply(e -> new TreeSet<>(e.keySet()));
  }

  @Override
  public CompletionStage<List<Topic>> topics(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());
    return to(kafkaAdmin
            .describeConfigs(
                topics.stream()
                    .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                    .collect(Collectors.toList()))
            .all())
        .thenCombine(
            to(kafkaAdmin.describeTopics(topics).all()),
            (configs, desc) ->
                configs.entrySet().stream()
                    .map(
                        entry ->
                            Topic.of(
                                entry.getKey().name(),
                                desc.get(entry.getKey().name()),
                                entry.getValue()))
                    .sorted(Comparator.comparing(Topic::name))
                    .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public CompletionStage<Void> deleteTopics(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(kafkaAdmin.deleteTopics(topics).all());
  }

  @Override
  public CompletionStage<Map<TopicPartition, Long>> deleteRecords(
      Map<TopicPartition, Long> offsets) {
    if (offsets.isEmpty()) return CompletableFuture.completedFuture(Map.of());
    return Utils.sequence(
            kafkaAdmin
                .deleteRecords(
                    offsets.entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                e -> TopicPartition.to(e.getKey()),
                                e -> RecordsToDelete.beforeOffset(e.getValue()))))
                .lowWatermarks()
                .entrySet()
                .stream()
                .map(
                    e ->
                        to(e.getValue().thenApply(r -> Map.entry(e.getKey(), r.lowWatermark())))
                            .toCompletableFuture())
                .collect(Collectors.toList()))
        .thenApply(
            r ->
                r.stream()
                    .collect(
                        Utils.toSortedMap(
                            e -> TopicPartition.from(e.getKey()), Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Set<TopicPartition>> topicPartitions(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(Set.of());
    return to(kafkaAdmin.describeTopics(topics).all())
        .thenApply(
            r ->
                r.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().partitions().stream()
                                .map(p -> TopicPartition.of(entry.getKey(), p.partition())))
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<Set<TopicPartitionReplica>> topicPartitionReplicas(Set<Integer> brokers) {
    if (brokers.isEmpty()) return CompletableFuture.completedFuture(Set.of());
    return topicNames(true)
        .thenCompose(topics -> to(kafkaAdmin.describeTopics(topics).all()))
        .thenApply(
            r ->
                r.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().partitions().stream()
                                .flatMap(
                                    p ->
                                        p.replicas().stream()
                                            .map(
                                                replica ->
                                                    TopicPartitionReplica.of(
                                                        entry.getKey(),
                                                        p.partition(),
                                                        replica.id()))))
                    .filter(replica -> brokers.contains(replica.brokerId()))
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<List<Partition>> partitions(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());
    var allPartitions =
        to(kafkaAdmin.describeTopics(topics).all())
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
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return allPartitions.thenCompose(
        partitionInfos -> {
          // kafka admin update metadata based on topic, so we skip topics hosted by offline node
          var availablePartitions =
              partitionInfos.entrySet().stream()
                  .collect(Collectors.groupingBy(e -> e.getKey().topic()))
                  .entrySet()
                  .stream()
                  .filter(
                      e ->
                          e.getValue().stream()
                              .allMatch(
                                  e2 ->
                                      e2.getValue().leader() != null
                                          && !e2.getValue().leader().isEmpty()))
                  .flatMap(e -> e.getValue().stream())
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          return to(kafkaAdmin
                  .listOffsets(
                      availablePartitions.keySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Function.identity(), e -> new OffsetSpec.EarliestSpec())))
                  .all())
              .thenCompose(
                  earliest ->
                      to(kafkaAdmin
                              .listOffsets(
                                  availablePartitions.keySet().stream()
                                      .collect(
                                          Collectors.toMap(
                                              Function.identity(),
                                              e -> new OffsetSpec.LatestSpec())))
                              .all())
                          .thenCompose(
                              latest ->
                                  to(kafkaAdmin
                                          .listOffsets(
                                              availablePartitions.keySet().stream()
                                                  .collect(
                                                      Collectors.toMap(
                                                          Function.identity(),
                                                          e -> new OffsetSpec.MaxTimestampSpec())))
                                          .all())
                                      // the old kafka does not support to fetch max timestamp
                                      .handle(
                                          (r, e) ->
                                              e == null
                                                  ? r
                                                  : Map
                                                      .<String,
                                                          ListOffsetsResult.ListOffsetsResultInfo>
                                                          of())
                                      .thenApply(
                                          maxTimestamp ->
                                              partitionInfos.entrySet().stream()
                                                  .map(
                                                      entry ->
                                                          Partition.of(
                                                              entry.getKey().topic(),
                                                              entry.getValue(),
                                                              Optional.ofNullable(
                                                                  earliest.get(entry.getKey())),
                                                              Optional.ofNullable(
                                                                  latest.get(entry.getKey())),
                                                              Optional.ofNullable(
                                                                  maxTimestamp.get(
                                                                      entry.getKey()))))
                                                  .sorted(
                                                      Comparator.comparing(Partition::topic)
                                                          .thenComparing(Partition::partition))
                                                  .collect(Collectors.toList()))));
        });
  }

  @Override
  public CompletionStage<Set<NodeInfo>> nodeInfos() {
    return to(kafkaAdmin.describeCluster().nodes())
        .thenApply(
            nodes ->
                nodes.stream().map(NodeInfo::of).collect(Collectors.toCollection(TreeSet::new)));
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
                                                                    .sorted(
                                                                        Comparator.comparing(
                                                                            NodeInfo::id))
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
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<List<ConsumerGroup>> consumerGroups(Set<String> consumerGroupIds) {
    if (consumerGroupIds.isEmpty()) return CompletableFuture.completedFuture(List.of());
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
                    .sorted(Comparator.comparing(ConsumerGroup::groupId))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<ProducerState>> producerStates(Set<TopicPartition> partitions) {
    if (partitions.isEmpty()) return CompletableFuture.completedFuture(List.of());
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
                    .sorted(Comparator.comparing(ProducerState::topic))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<AddingReplica>> addingReplicas(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());
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
                  .sorted(
                      Comparator.comparing(AddingReplica::topic)
                          .thenComparing(AddingReplica::partition)
                          .thenComparing(AddingReplica::broker))
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
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<List<Transaction>> transactions(Set<String> transactionIds) {
    if (transactionIds.isEmpty()) return CompletableFuture.completedFuture(List.of());
    return to(kafkaAdmin.describeTransactions(transactionIds).all())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    .map(e -> Transaction.of(e.getKey(), e.getValue()))
                    .sorted(Comparator.comparing(Transaction::transactionId))
                    .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public CompletionStage<List<Replica>> replicas(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());
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
                    .sorted(
                        Comparator.comparing(Replica::topic)
                            .thenComparing(Replica::partition)
                            .thenComparing(r -> r.nodeInfo().id()))
                    .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<Quota>> quotas(String targetKey) {
    return to(kafkaAdmin
            .describeClientQuotas(
                ClientQuotaFilter.contains(
                    List.of(ClientQuotaFilterComponent.ofEntityType(targetKey))))
            .entities())
        .thenApply(Quota::of);
  }

  @Override
  public CompletionStage<List<Quota>> quotas() {
    return to(kafkaAdmin.describeClientQuotas(ClientQuotaFilter.all()).entities())
        .thenApply(Quota::of);
  }

  @Override
  public CompletionStage<Void> setConnectionQuotas(Map<String, Integer> ipAndRate) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                ipAndRate.entrySet().stream()
                    .map(
                        entry ->
                            new org.apache.kafka.common.quota.ClientQuotaAlteration(
                                new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, entry.getKey())),
                                List.of(
                                    new ClientQuotaAlteration.Op(
                                        QuotaConfigs.IP_CONNECTION_RATE_CONFIG,
                                        (double) entry.getValue()))))
                    .collect(Collectors.toList()))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetConnectionQuotas(Set<String> ips) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                ips.stream()
                    .map(
                        ip ->
                            new org.apache.kafka.common.quota.ClientQuotaAlteration(
                                new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, ip)),
                                List.of(
                                    new ClientQuotaAlteration.Op(
                                        QuotaConfigs.IP_CONNECTION_RATE_CONFIG, null))))
                    .collect(Collectors.toList()))
            .all());
  }

  @Override
  public CompletionStage<Void> setConsumerQuotas(Map<String, DataRate> ipAndRate) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                ipAndRate.entrySet().stream()
                    .map(
                        entry ->
                            new org.apache.kafka.common.quota.ClientQuotaAlteration(
                                new ClientQuotaEntity(
                                    Map.of(ClientQuotaEntity.CLIENT_ID, entry.getKey())),
                                List.of(
                                    new ClientQuotaAlteration.Op(
                                        QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG,
                                        entry.getValue().byteRate()))))
                    .collect(Collectors.toList()))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetConsumerQuotas(Set<String> clientIds) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                clientIds.stream()
                    .map(
                        clientId ->
                            new org.apache.kafka.common.quota.ClientQuotaAlteration(
                                new ClientQuotaEntity(
                                    Map.of(ClientQuotaEntity.CLIENT_ID, clientId)),
                                List.of(
                                    new ClientQuotaAlteration.Op(
                                        QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG, null))))
                    .collect(Collectors.toList()))
            .all());
  }

  @Override
  public CompletionStage<Void> setProducerQuotas(Map<String, DataRate> ipAndRate) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                ipAndRate.entrySet().stream()
                    .map(
                        entry -> {
                          System.out.println(
                              "entry.getValue().byteRate(): " + entry.getValue().byteRate());
                          return new org.apache.kafka.common.quota.ClientQuotaAlteration(
                              new ClientQuotaEntity(
                                  Map.of(ClientQuotaEntity.CLIENT_ID, entry.getKey())),
                              List.of(
                                  new ClientQuotaAlteration.Op(
                                      QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG,
                                      entry.getValue().byteRate())));
                        })
                    .collect(Collectors.toList()))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetProducerQuotas(Set<String> clientIds) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                clientIds.stream()
                    .map(
                        clientId ->
                            new org.apache.kafka.common.quota.ClientQuotaAlteration(
                                new ClientQuotaEntity(
                                    Map.of(ClientQuotaEntity.CLIENT_ID, clientId)),
                                List.of(
                                    new ClientQuotaAlteration.Op(
                                        QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG, null))))
                    .collect(Collectors.toList()))
            .all());
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
  public CompletionStage<Void> moveToBrokers(Map<TopicPartition, List<Integer>> assignments) {
    if (assignments.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(
        kafkaAdmin
            .alterPartitionReassignments(
                assignments.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> TopicPartition.to(e.getKey()),
                            e -> Optional.of(new NewPartitionReassignment(e.getValue())))))
            .all());
  }

  @Override
  public CompletionStage<Void> moveToFolders(Map<TopicPartitionReplica, String> assignments) {
    if (assignments.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(
        kafkaAdmin
            .alterReplicaLogDirs(
                assignments.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> TopicPartitionReplica.to(e.getKey()), Map.Entry::getValue)))
            .all());
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
  public CompletionStage<Void> setConfigs(String topic, Map<String, String> override) {
    if (override.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(
        kafkaAdmin
            .incrementalAlterConfigs(
                Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, topic),
                    override.entrySet().stream()
                        .map(
                            entry ->
                                new AlterConfigOp(
                                    new ConfigEntry(entry.getKey(), entry.getValue()),
                                    AlterConfigOp.OpType.SET))
                        .collect(Collectors.toList())))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetConfigs(String topic, Set<String> keys) {
    if (keys.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(
        kafkaAdmin
            .incrementalAlterConfigs(
                Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, topic),
                    keys.stream()
                        .map(
                            key ->
                                new AlterConfigOp(
                                    new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE))
                        .collect(Collectors.toList())))
            .all());
  }

  @Override
  public CompletionStage<Void> setConfigs(int brokerId, Map<String, String> override) {
    if (override.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(
        kafkaAdmin
            .incrementalAlterConfigs(
                Map.of(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
                    override.entrySet().stream()
                        .map(
                            entry ->
                                new AlterConfigOp(
                                    new ConfigEntry(entry.getKey(), entry.getValue()),
                                    AlterConfigOp.OpType.SET))
                        .collect(Collectors.toList())))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetConfigs(int brokerId, Set<String> keys) {
    if (keys.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(
        kafkaAdmin
            .incrementalAlterConfigs(
                Map.of(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)),
                    keys.stream()
                        .map(
                            key ->
                                new AlterConfigOp(
                                    new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE))
                        .collect(Collectors.toList())))
            .all());
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
