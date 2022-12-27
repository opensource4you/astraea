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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.common.DataRate;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.Utils;

class AdminImpl implements Admin {

  private final org.apache.kafka.clients.admin.Admin kafkaAdmin;
  private final String clientId;
  private final AtomicInteger runningRequests = new AtomicInteger(0);

  AdminImpl(Map<String, String> props) {
    this(
        KafkaAdminClient.create(
            props.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }

  AdminImpl(org.apache.kafka.clients.admin.Admin kafkaAdmin) {
    this.kafkaAdmin = kafkaAdmin;
    this.clientId = (String) Utils.member(kafkaAdmin, "clientId");
  }

  <T> CompletionStage<T> to(org.apache.kafka.common.KafkaFuture<T> kafkaFuture) {
    runningRequests.incrementAndGet();
    var f = new CompletableFuture<T>();
    kafkaFuture.whenComplete(
        (r, e) -> {
          runningRequests.decrementAndGet();
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
  public int runningRequests() {
    return runningRequests.get();
  }

  @Override
  public CompletionStage<Set<String>> topicNames(boolean listInternal) {
    return to(kafkaAdmin
            .listTopics(new ListTopicsOptions().listInternal(listInternal))
            .namesToListings())
        .thenApply(e -> new TreeSet<>(e.keySet()));
  }

  @Override
  public CompletionStage<Set<String>> internalTopicNames() {
    return to(kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true)).namesToListings())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    .filter(t -> t.getValue().isInternal())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<List<Topic>> topics(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());

    return FutureUtils.combine(
        doGetConfigs(
            topics.stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                .collect(Collectors.toList())),
        to(kafkaAdmin.describeTopics(topics).all()),
        (configs, desc) ->
            configs.entrySet().stream()
                .map(entry -> Topic.of(entry.getKey(), desc.get(entry.getKey()), entry.getValue()))
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
    return FutureUtils.sequence(
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
                        MapUtils.toSortedMap(
                            e -> TopicPartition.from(e.getKey()), Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Void> deleteInstanceMembers(Map<String, Set<String>> groupAndInstanceIds) {
    return FutureUtils.sequence(
            groupAndInstanceIds.entrySet().stream()
                .map(
                    entry ->
                        to(kafkaAdmin
                                .removeMembersFromConsumerGroup(
                                    entry.getKey(),
                                    new RemoveMembersFromConsumerGroupOptions(
                                        entry.getValue().stream()
                                            .map(MemberToRemove::new)
                                            .collect(Collectors.toList())))
                                .all())
                            .toCompletableFuture())
                .collect(Collectors.toList()))
        .thenApply(ignored -> null);
  }

  @Override
  public CompletionStage<Void> deleteMembers(Set<String> consumerGroups) {
    // kafka APIs disallow to remove all members when there are no members ...
    // Hence, we have to filter the non-empty groups first.
    return to(kafkaAdmin.describeConsumerGroups(consumerGroups).all())
        .thenApply(
            groups ->
                groups.entrySet().stream()
                    .filter(g -> !g.getValue().members().isEmpty())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet()))
        .thenCompose(
            groups ->
                FutureUtils.sequence(
                    groups.stream()
                        .map(
                            group ->
                                to(kafkaAdmin
                                        .removeMembersFromConsumerGroup(
                                            group, new RemoveMembersFromConsumerGroupOptions())
                                        .all())
                                    .toCompletableFuture())
                        .collect(Collectors.toList())))
        .thenApply(ignored -> null);
  }

  @Override
  public CompletionStage<Void> deleteGroups(Set<String> consumerGroups) {
    return deleteMembers(consumerGroups)
        .thenCompose(ignored -> to(kafkaAdmin.deleteConsumerGroups(consumerGroups).all()));
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

  /**
   * Some requests get blocked when there are offline brokers. In order to avoid blocking, this
   * method returns the fetchable partitions.
   */
  private CompletionStage<Set<TopicPartition>> updatableTopicPartitions(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(Set.of());
    return to(kafkaAdmin.describeTopics(topics).all())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    // kafka update metadata based on topic, so it may get blocked if there is one
                    // offline partition
                    .filter(
                        e ->
                            e.getValue().partitions().stream()
                                .allMatch(tp -> tp.leader() != null && !tp.leader().isEmpty()))
                    .flatMap(
                        e ->
                            e.getValue().partitions().stream()
                                .map(tp -> TopicPartition.of(e.getKey(), tp.partition())))
                    .collect(Collectors.toSet()));
  }

  @Override
  public CompletionStage<Map<TopicPartition, Long>> earliestOffsets(
      Set<TopicPartition> topicPartitions) {
    if (topicPartitions.isEmpty()) return CompletableFuture.completedFuture(Map.of());
    var updatableTopicPartitions =
        updatableTopicPartitions(
            topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
    return FutureUtils.combine(
        updatableTopicPartitions,
        updatableTopicPartitions.thenCompose(
            ps ->
                to(
                    kafkaAdmin
                        .listOffsets(
                            ps.stream()
                                .collect(
                                    Collectors.toMap(
                                        TopicPartition::to,
                                        ignored -> new OffsetSpec.EarliestSpec())))
                        .all())),
        (ps, result) ->
            ps.stream()
                .collect(
                    Collectors.toMap(
                        tp -> tp,
                        tp ->
                            Optional.ofNullable(result.get(TopicPartition.to(tp)))
                                .map(ListOffsetsResult.ListOffsetsResultInfo::offset)
                                .orElse(-1L))));
  }

  @Override
  public CompletionStage<Map<TopicPartition, Long>> latestOffsets(
      Set<TopicPartition> topicPartitions) {
    if (topicPartitions.isEmpty()) return CompletableFuture.completedFuture(Map.of());
    var updatableTopicPartitions =
        updatableTopicPartitions(
            topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
    return FutureUtils.combine(
        updatableTopicPartitions,
        updatableTopicPartitions.thenCompose(
            ps ->
                to(
                    kafkaAdmin
                        .listOffsets(
                            ps.stream()
                                .collect(
                                    Collectors.toMap(
                                        TopicPartition::to,
                                        ignored -> new OffsetSpec.LatestSpec())))
                        .all())),
        (ps, result) ->
            ps.stream()
                .collect(
                    Collectors.toMap(
                        tp -> tp,
                        tp ->
                            Optional.ofNullable(result.get(TopicPartition.to(tp)))
                                .map(ListOffsetsResult.ListOffsetsResultInfo::offset)
                                .orElse(-1L))));
  }

  @Override
  public CompletionStage<Map<TopicPartition, Long>> maxTimestamps(
      Set<TopicPartition> topicPartitions) {
    if (topicPartitions.isEmpty()) return CompletableFuture.completedFuture(Map.of());
    var updatableTopicPartitions =
        updatableTopicPartitions(
            topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
    return FutureUtils.combine(
        updatableTopicPartitions,
        updatableTopicPartitions.thenCompose(
            ps ->
                to(
                    kafkaAdmin
                        .listOffsets(
                            ps.stream()
                                .collect(
                                    Collectors.toMap(
                                        TopicPartition::to,
                                        ignored -> new OffsetSpec.MaxTimestampSpec())))
                        .all())),
        (ps, result) ->
            ps.stream()
                .flatMap(
                    tp ->
                        Optional.ofNullable(result.get(TopicPartition.to(tp))).stream()
                            .map(ListOffsetsResult.ListOffsetsResultInfo::timestamp)
                            .filter(t -> t > 0)
                            .map(t -> Map.entry(tp, t)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<List<Partition>> partitions(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());
    var updatableTopicPartitions = updatableTopicPartitions(topics);
    var topicDesc = to(kafkaAdmin.describeTopics(topics).all());
    return FutureUtils.combine(
        updatableTopicPartitions.thenCompose(this::earliestOffsets),
        updatableTopicPartitions.thenCompose(this::latestOffsets),
        updatableTopicPartitions
            .thenCompose(this::maxTimestamps)
            // the old kafka does not support to fetch max timestamp. It is fine to return partition
            // without max timestamp
            .exceptionally(
                e -> {
                  if (e instanceof UnsupportedVersionException
                      || e.getCause() instanceof UnsupportedVersionException) return Map.of();
                  if (e instanceof RuntimeException) throw (RuntimeException) e;
                  throw new RuntimeException(e);
                }),
        topicDesc.thenApply(
            ts ->
                ts.entrySet().stream()
                    .flatMap(
                        e ->
                            e.getValue().partitions().stream()
                                .map(
                                    tp ->
                                        Map.entry(
                                            TopicPartition.of(e.getKey(), tp.partition()), tp)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
        topicDesc.thenApply(
            ts ->
                ts.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().isInternal()))),
        (earliestOffsets, latestOffsets, maxTimestamps, tpInfos, topicAndInternal) ->
            tpInfos.keySet().stream()
                .map(
                    tp -> {
                      var earliest = earliestOffsets.getOrDefault(tp, -1L);
                      var latest = latestOffsets.getOrDefault(tp, -1L);
                      var maxTimestamp = Optional.ofNullable(maxTimestamps.get(tp));
                      var tpInfo = tpInfos.get(tp);
                      var leader =
                          tpInfo.leader() == null || tpInfo.leader().isEmpty()
                              ? null
                              : NodeInfo.of(tpInfo.leader());
                      var replicas =
                          tpInfo.replicas().stream().map(NodeInfo::of).collect(Collectors.toList());
                      var isr =
                          tpInfo.isr().stream().map(NodeInfo::of).collect(Collectors.toList());
                      return Partition.of(
                          tp.topic(),
                          tp.partition(),
                          leader,
                          replicas,
                          isr,
                          earliest,
                          latest,
                          maxTimestamp,
                          topicAndInternal.get(tp.topic()));
                    })
                .sorted(Comparator.comparing(Partition::topicPartition))
                .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<NodeInfo>> nodeInfos() {
    return to(kafkaAdmin.describeCluster().nodes())
        .thenApply(
            nodes -> nodes.stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public CompletionStage<List<Broker>> brokers() {
    var cluster = kafkaAdmin.describeCluster();
    var nodeFuture = to(cluster.nodes());
    return FutureUtils.combine(
        to(cluster.controller()),
        topicNames(true).thenCompose(names -> to(kafkaAdmin.describeTopics(names).all())),
        nodeFuture.thenCompose(
            nodes ->
                to(
                    kafkaAdmin
                        .describeLogDirs(nodes.stream().map(Node::id).collect(Collectors.toList()))
                        .all())),
        nodeFuture.thenCompose(
            nodes ->
                doGetConfigs(
                    nodes.stream()
                        .map(
                            n ->
                                new ConfigResource(
                                    ConfigResource.Type.BROKER, String.valueOf(n.id())))
                        .collect(Collectors.toList()))),
        nodeFuture,
        (controller, topics, logDirs, configs, nodes) ->
            nodes.stream()
                .map(
                    node ->
                        Broker.of(
                            node.id() == controller.id(),
                            node,
                            configs.get(String.valueOf(node.id())),
                            logDirs.get(node.id()),
                            topics.values()))
                .sorted(Comparator.comparing(NodeInfo::id))
                .collect(Collectors.toList()));
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
    return FutureUtils.combine(
        to(kafkaAdmin.describeConsumerGroups(consumerGroupIds).all()),
        FutureUtils.sequence(
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
                s -> s.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
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
            .describeTopics(
                partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()))
            .all())
        .thenApply(
            ts ->
                partitions.stream()
                    .filter(
                        p ->
                            ts.containsKey(p.topic())
                                && ts.get(p.topic()).partitions().stream()
                                    .anyMatch(
                                        partitionInfo ->
                                            partitionInfo.partition() == p.partition()
                                                // It will get stuck if admin tries to connect to
                                                // offline nodes,
                                                && partitionInfo.leader() != null
                                                && !partitionInfo.leader().isEmpty()))
                    .collect(Collectors.toSet()))
        .thenCompose(
            availablePartitions ->
                to(kafkaAdmin
                        .describeProducers(
                            availablePartitions.stream()
                                .map(TopicPartition::to)
                                .collect(Collectors.toUnmodifiableList()))
                        .all())
                    .exceptionally(
                        e -> {
                          // supported version: 2.8.0
                          // https://issues.apache.org/jira/browse/KAFKA-12238
                          if (e instanceof UnsupportedVersionException
                              || e.getCause() instanceof UnsupportedVersionException)
                            return Map.of();

                          if (e instanceof RuntimeException) throw (RuntimeException) e;
                          throw new RuntimeException(e);
                        })
                    .thenApply(
                        ps ->
                            ps.entrySet().stream()
                                .flatMap(
                                    e ->
                                        e.getValue().activeProducers().stream()
                                            .map(s -> ProducerState.of(e.getKey(), s)))
                                .sorted(Comparator.comparing(ProducerState::topic))
                                .collect(Collectors.toList())));
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
  public CompletionStage<ClusterInfo<Replica>> clusterInfo(Set<String> topics) {
    return FutureUtils.combine(
        brokers()
            .thenApply(
                brokers ->
                    brokers.stream()
                        .map(x -> (NodeInfo) x)
                        .collect(Collectors.toUnmodifiableList())),
        replicas(topics),
        ClusterInfo::of);
  }

  private CompletionStage<List<Replica>> replicas(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());

    // pre-group folders by (broker -> topic partition) to speedup seek
    return FutureUtils.combine(
        logDirs(),
        to(kafkaAdmin.describeTopics(topics).allTopicNames()),
        to(kafkaAdmin.listPartitionReassignments().reassignments()),
        (logDirs, ts, reassignmentMap) ->
            ts.values().stream()
                .flatMap(topic -> topic.partitions().stream().map(p -> Map.entry(topic.name(), p)))
                .flatMap(
                    topicAndPartition ->
                        topicAndPartition.getValue().replicas().stream()
                            .flatMap(
                                node -> {
                                  var topicName = topicAndPartition.getKey();
                                  var internal = ts.get(topicName).isInternal();
                                  var partition = topicAndPartition.getValue();
                                  var partitionId = topicAndPartition.getValue().partition();
                                  var reassignment =
                                      reassignmentMap.get(
                                          new org.apache.kafka.common.TopicPartition(
                                              topicName, partitionId));
                                  var isAdding =
                                      reassignment != null
                                          && reassignment.addingReplicas().contains(node.id());
                                  var isRemoving =
                                      reassignment != null
                                          && reassignment.removingReplicas().contains(node.id());
                                  // kafka admin#describeLogDirs does not return
                                  // offline node,when the node is not online,all
                                  // TopicPartition return an empty dataFolder and a
                                  // fake replicaInfo, and determine whether the
                                  // node is online by whether the dataFolder is "".
                                  var pathAndReplicas =
                                      logDirs
                                          .getOrDefault(node.id(), Map.of())
                                          .getOrDefault(
                                              TopicPartition.of(topicName, partitionId),
                                              Map.of(
                                                  "",
                                                  new org.apache.kafka.clients.admin.ReplicaInfo(
                                                      -1L, -1L, false)));
                                  return pathAndReplicas.entrySet().stream()
                                      .map(
                                          pathAndReplica ->
                                              Replica.builder()
                                                  .topic(topicName)
                                                  .partition(partitionId)
                                                  .internal(internal)
                                                  .isAdding(isAdding)
                                                  .isRemoving(isRemoving)
                                                  .nodeInfo(NodeInfo.of(node))
                                                  .lag(pathAndReplica.getValue().offsetLag())
                                                  .size(pathAndReplica.getValue().size())
                                                  .isLeader(
                                                      partition.leader() != null
                                                          && !partition.leader().isEmpty()
                                                          && partition.leader().id() == node.id())
                                                  .inSync(partition.isr().contains(node))
                                                  .isFuture(pathAndReplica.getValue().isFuture())
                                                  .isOffline(
                                                      node.isEmpty()
                                                          || pathAndReplica.getKey().equals(""))
                                                  // The first replica in the return result is the
                                                  // preferred leader. This only works with Kafka
                                                  // broker version after 0.11. Version before
                                                  // 0.11 returns the replicas in unspecified order.
                                                  .isPreferredLeader(
                                                      partition.replicas().get(0).id() == node.id())
                                                  // empty data folder means this
                                                  // replica is offline
                                                  .path(
                                                      pathAndReplica.getKey().isEmpty()
                                                          ? null
                                                          : pathAndReplica.getKey())
                                                  .build());
                                }))
                .sorted(
                    Comparator.comparing(Replica::topic)
                        .thenComparing(Replica::partition)
                        .thenComparing(r -> r.nodeInfo().id()))
                .collect(Collectors.toUnmodifiableList()));
  }

  @Override
  public CompletionStage<List<Quota>> quotas(Map<String, Set<String>> targets) {
    return to(kafkaAdmin
            .describeClientQuotas(
                ClientQuotaFilter.contains(
                    targets.entrySet().stream()
                        .flatMap(
                            t ->
                                t.getValue().stream()
                                    .map(v -> ClientQuotaFilterComponent.ofEntity(t.getKey(), v)))
                        .collect(Collectors.toList())))
            .entities())
        .thenApply(Quota::of);
  }

  @Override
  public CompletionStage<List<Quota>> quotas(Set<String> targetKeys) {
    return to(kafkaAdmin
            .describeClientQuotas(
                ClientQuotaFilter.contains(
                    targetKeys.stream()
                        .map(ClientQuotaFilterComponent::ofEntityType)
                        .collect(Collectors.toList())))
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
                        entry ->
                            new ClientQuotaAlteration(
                                new ClientQuotaEntity(
                                    Map.of(ClientQuotaEntity.CLIENT_ID, entry.getKey())),
                                List.of(
                                    new ClientQuotaAlteration.Op(
                                        QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG,
                                        entry.getValue().byteRate()))))
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
        this.topic = Utils.requireNonEmpty(topic);
        return this;
      }

      @Override
      public TopicCreator numberOfPartitions(int numberOfPartitions) {
        this.numberOfPartitions = Utils.requirePositive(numberOfPartitions);
        return this;
      }

      @Override
      public TopicCreator numberOfReplicas(short numberOfReplicas) {
        this.numberOfReplicas = Utils.requirePositive(numberOfReplicas);
        return this;
      }

      @Override
      public TopicCreator configs(Map<String, String> configs) {
        this.configs = Objects.requireNonNull(configs);
        return this;
      }

      @Override
      public CompletionStage<Boolean> run() {
        Utils.requireNonEmpty(topic);

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
                  return FutureUtils.combine(
                      topics(Set.of(topic)),
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
  public CompletionStage<Void> preferredLeaderElection(Set<TopicPartition> partitions) {
    return to(kafkaAdmin
            .electLeaders(
                ElectionType.PREFERRED,
                partitions.stream().map(TopicPartition::to).collect(Collectors.toSet()))
            .all())
        .exceptionally(
            e -> {
              // This error occurred if the preferred leader of the given topic/partition is already
              // the leader. It is ok to swallow the exception since the preferred leader be the
              // actual leader. That is what the caller wants to be.
              if (e instanceof ElectionNotNeededException) return null;
              if (e instanceof RuntimeException) throw (RuntimeException) e;
              throw new RuntimeException(e);
            });
  }

  @Override
  public CompletionStage<Void> addPartitions(String topic, int total) {
    return to(kafkaAdmin.createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
  }

  @Override
  public CompletionStage<Void> setTopicConfigs(Map<String, Map<String, String>> override) {
    return doSetConfigs(
        override.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new ConfigResource(ConfigResource.Type.TOPIC, e.getKey()),
                    Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Void> appendTopicConfigs(Map<String, Map<String, String>> appended) {
    return doAppendConfigs(
        appended.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new ConfigResource(ConfigResource.Type.TOPIC, e.getKey()),
                    Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Void> subtractTopicConfigs(Map<String, Map<String, String>> subtracted) {
    return doSubtractConfigs(
        subtracted.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new ConfigResource(ConfigResource.Type.TOPIC, e.getKey()),
                    Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Void> unsetTopicConfigs(Map<String, Set<String>> unset) {
    return doUnsetConfigs(
        unset.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new ConfigResource(ConfigResource.Type.TOPIC, e.getKey()),
                    Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Void> setBrokerConfigs(Map<Integer, Map<String, String>> override) {
    return doSetConfigs(
        override.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(e.getKey())),
                    Map.Entry::getValue)));
  }

  @Override
  public CompletionStage<Void> unsetBrokerConfigs(Map<Integer, Set<String>> unset) {
    return doUnsetConfigs(
        unset.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(e.getKey())),
                    Map.Entry::getValue)));
  }

  private CompletionStage<Map<String, Map<String, String>>> doGetConfigs(
      Collection<ConfigResource> resources) {
    return to(kafkaAdmin.describeConfigs(resources).all())
        .thenApply(
            allConfigs ->
                allConfigs.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> e.getKey().name(),
                            e ->
                                e.getValue().entries().stream()
                                    .filter(
                                        entry -> entry.value() != null && !entry.value().isBlank())
                                    .collect(
                                        Collectors.toMap(ConfigEntry::name, ConfigEntry::value)))));
  }

  private CompletionStage<Void> doAppendConfigs(Map<ConfigResource, Map<String, String>> append) {

    var nonEmptyAppend =
        append.entrySet().stream()
            .filter(r -> !r.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (nonEmptyAppend.isEmpty()) return CompletableFuture.completedFuture(null);

    return doGetConfigs(nonEmptyAppend.keySet())
        .thenCompose(
            allConfigs -> {
              // append to empty will cause bug (see https://github.com/apache/kafka/pull/12503)
              var requestToSet =
                  nonEmptyAppend.entrySet().stream()
                      .map(
                          config ->
                              Map.entry(
                                  config.getKey(),
                                  config.getValue().entrySet().stream()
                                      .filter(
                                          entry ->
                                              !allConfigs
                                                  .getOrDefault(config.getKey().name(), Map.of())
                                                  .containsKey(entry.getKey()))
                                      .collect(
                                          Collectors.toMap(
                                              Map.Entry::getKey, Map.Entry::getValue))))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              // append value only if 1) it is not wildcard, 2) it is not empty
              Map<ConfigResource, Collection<AlterConfigOp>> requestToAppend =
                  nonEmptyAppend.entrySet().stream()
                      .map(
                          config ->
                              Map.entry(
                                  config.getKey(),
                                  config.getValue().entrySet().stream()
                                      .filter(
                                          entry ->
                                              allConfigs
                                                      .getOrDefault(
                                                          config.getKey().name(), Map.of())
                                                      .containsKey(entry.getKey())
                                                  && Arrays.stream(
                                                          allConfigs
                                                              .getOrDefault(
                                                                  config.getKey().name(), Map.of())
                                                              .get(entry.getKey())
                                                              .split(","))
                                                      .noneMatch(
                                                          s ->
                                                              s.equals("*")
                                                                  || s.equals(entry.getValue())))
                                      .collect(
                                          Collectors.toMap(
                                              Map.Entry::getKey, Map.Entry::getValue))))
                      .filter(r -> !r.getValue().isEmpty())
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e ->
                                  e.getValue().entrySet().stream()
                                      .map(
                                          r ->
                                              new AlterConfigOp(
                                                  new ConfigEntry(r.getKey(), r.getValue()),
                                                  AlterConfigOp.OpType.APPEND))
                                      .collect(Collectors.toList())));

              return doSetConfigs(requestToSet)
                  .thenCompose(
                      ignored -> to(kafkaAdmin.incrementalAlterConfigs(requestToAppend).all()));
            });
  }

  private CompletionStage<Void> doSubtractConfigs(
      Map<ConfigResource, Map<String, String>> subtract) {

    var nonEmptySubtract =
        subtract.entrySet().stream()
            .filter(r -> !r.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (nonEmptySubtract.isEmpty()) return CompletableFuture.completedFuture(null);

    return doGetConfigs(nonEmptySubtract.keySet())
        .thenCompose(
            configs -> {
              Map<ConfigResource, Collection<AlterConfigOp>> requestToSubtract =
                  nonEmptySubtract.entrySet().stream()
                      .map(
                          e ->
                              Map.entry(
                                  e.getKey(),
                                  e.getValue().entrySet().stream()
                                      .filter(
                                          entry -> {
                                            // nothing to subtract
                                            if (!configs
                                                .getOrDefault(e.getKey().name(), Map.of())
                                                .containsKey(entry.getKey())) return false;
                                            var values =
                                                Arrays.stream(
                                                        configs
                                                            .getOrDefault(
                                                                e.getKey().name(), Map.of())
                                                            .getOrDefault(entry.getKey(), "")
                                                            .split(","))
                                                    .collect(Collectors.toList());
                                            // disable to subtract from *
                                            if (values.contains("*"))
                                              throw new IllegalArgumentException(
                                                  "Can't subtract config from \"*\", key: "
                                                      + entry.getKey()
                                                      + ", value: "
                                                      + configs.containsKey(entry.getKey()));
                                            return values.contains(entry.getValue());
                                          })
                                      .collect(
                                          Collectors.toMap(
                                              Map.Entry::getKey, Map.Entry::getValue))))
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              r ->
                                  r.getValue().entrySet().stream()
                                      .map(
                                          e ->
                                              new AlterConfigOp(
                                                  new ConfigEntry(e.getKey(), e.getValue()),
                                                  AlterConfigOp.OpType.SUBTRACT))
                                      .collect(Collectors.toList())));
              return to(kafkaAdmin.incrementalAlterConfigs(requestToSubtract).all());
            });
  }

  private CompletionStage<Void> doSetConfigs(Map<ConfigResource, Map<String, String>> override) {

    var nonEmptyOverride =
        override.entrySet().stream()
            .filter(e -> !e.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (nonEmptyOverride.isEmpty()) return CompletableFuture.completedFuture(null);

    Supplier<Map<ConfigResource, Collection<AlterConfigOp>>> newVersion =
        () ->
            nonEmptyOverride.entrySet().stream()
                .map(
                    e ->
                        Map.entry(
                            e.getKey(),
                            e.getValue().entrySet().stream()
                                .map(
                                    entry ->
                                        new AlterConfigOp(
                                            new ConfigEntry(entry.getKey(), entry.getValue()),
                                            AlterConfigOp.OpType.SET))
                                .collect(Collectors.toList())))
                .filter(e -> !e.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Supplier<Map<ConfigResource, Config>> previousVersion =
        () ->
            nonEmptyOverride.entrySet().stream()
                .map(
                    e ->
                        Map.entry(
                            e.getKey(),
                            new org.apache.kafka.clients.admin.Config(
                                e.getValue().entrySet().stream()
                                    .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue()))
                                    .collect(Collectors.toList()))))
                .filter(e -> !e.getValue().entries().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return doChangeConfigs(newVersion, previousVersion);
  }

  private CompletionStage<Void> doUnsetConfigs(Map<ConfigResource, Set<String>> unset) {
    var nonEmptyUnset =
        unset.entrySet().stream()
            .filter(e -> !e.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (nonEmptyUnset.isEmpty()) return CompletableFuture.completedFuture(null);

    Supplier<Map<ConfigResource, Collection<AlterConfigOp>>> newVersion =
        () ->
            nonEmptyUnset.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e ->
                            e.getValue().stream()
                                .map(
                                    key ->
                                        new AlterConfigOp(
                                            new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE))
                                .collect(Collectors.toList())));

    Supplier<Map<ConfigResource, Config>> previousVersion =
        () ->
            nonEmptyUnset.entrySet().stream()
                .map(
                    e ->
                        Map.entry(
                            e.getKey(),
                            new org.apache.kafka.clients.admin.Config(
                                e.getValue().stream()
                                    .map(key -> new ConfigEntry(key, ""))
                                    .collect(Collectors.toList()))))
                .filter(e -> !e.getValue().entries().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return doChangeConfigs(newVersion, previousVersion);
  }

  private CompletionStage<Void> doChangeConfigs(
      Supplier<Map<ConfigResource, Collection<AlterConfigOp>>> newVersion,
      Supplier<Map<ConfigResource, Config>> previousVersion) {
    return to(kafkaAdmin.incrementalAlterConfigs(newVersion.get()).all())
        .handle(
            (r, e) -> {
              if (e != null) {
                if (e instanceof UnsupportedVersionException
                    || e.getCause() instanceof UnsupportedVersionException)
                  // go back to use deprecated APIs for previous Kafka
                  return to(kafkaAdmin.alterConfigs(previousVersion.get()).all());

                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new RuntimeException(e);
              }
              return CompletableFuture.<Void>completedStage(null);
            })
        .thenCompose(s -> s);
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
