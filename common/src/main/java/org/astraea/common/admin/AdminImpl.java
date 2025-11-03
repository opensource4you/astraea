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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
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
  private final org.apache.kafka.clients.admin.Admin controllerAdmin;
  private final String clientId;
  private final AtomicInteger runningRequests = new AtomicInteger(0);

  AdminImpl(Map<String, String> props) {
    this(
        org.apache.kafka.clients.admin.Admin.create(
            props.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }

  AdminImpl(org.apache.kafka.clients.admin.Admin kafkaAdmin) {
    this.kafkaAdmin = kafkaAdmin;
    this.clientId = (String) Utils.member(kafkaAdmin, "clientId");
    this.controllerAdmin =
        org.apache.kafka.clients.admin.Admin.create(
            Map.of(
                "bootstrap.controllers",
                kafkaAdmin
                    .describeMetadataQuorum()
                    .quorumInfo()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join()
                    .nodes()
                    .values()
                    .stream()
                    .flatMap(n -> n.endpoints().stream())
                    .map(e -> e.host() + ":" + e.port())
                    .collect(Collectors.joining(","))));
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

  private org.apache.kafka.clients.admin.Admin admin(boolean fromController) {
    return fromController ? controllerAdmin : kafkaAdmin;
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
  public CompletionStage<Void> feature(Map<String, Short> maxVersions) {
    return to(
        kafkaAdmin
            .updateFeatures(
                maxVersions.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            e ->
                                new FeatureUpdate(
                                    e.getValue(), FeatureUpdate.UpgradeType.UPGRADE))),
                new UpdateFeaturesOptions())
            .all());
  }

  @Override
  public CompletionStage<FeatureInfo> feature() {
    return to(admin(false).describeFeatures().featureMetadata())
        .thenApply(
            f ->
                new FeatureInfo(
                    f.finalizedFeatures().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                    new VersionRange(
                                        e.getValue().minVersionLevel(),
                                        e.getValue().maxVersionLevel()))),
                    f.finalizedFeaturesEpoch(),
                    f.supportedFeatures().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                    new VersionRange(
                                        e.getValue().minVersion(), e.getValue().maxVersion())))));
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
    return to(admin(false).listTopics(new ListTopicsOptions().listInternal(true)).namesToListings())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    .filter(t -> t.getValue().isInternal())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<List<Topic>> topics(Set<String> topics, boolean fromController) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());
    return FutureUtils.combine(
        doGetConfigs(
            topics.stream()
                .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                .collect(Collectors.toList()),
            fromController),
        // Quorum controller does not support DescribeTopicPartitionsRequest
        to(admin(false).describeTopics(topics).allTopicNames()),
        (configs, desc) ->
            configs.entrySet().stream()
                .map(entry -> Topic.of(entry.getKey(), desc.get(entry.getKey()), entry.getValue()))
                .sorted(Comparator.comparing(Topic::name))
                .toList());
  }

  @Override
  public CompletionStage<Void> deleteTopics(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(null);
    return to(admin(false).deleteTopics(topics).all());
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
    return to(admin(false).describeConsumerGroups(consumerGroups).all())
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
        .thenCompose(ignored -> to(admin(false).deleteConsumerGroups(consumerGroups).all()));
  }

  @Override
  public CompletionStage<Set<TopicPartition>> topicPartitions(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(Set.of());
    return to(admin(false).describeTopics(topics).allTopicNames())
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
        .thenCompose(topics -> to(admin(false).describeTopics(topics).allTopicNames()))
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
    return to(admin(false).describeTopics(topics).allTopicNames())
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
    var topicDesc = to(admin(false).describeTopics(topics).allTopicNames());
    return FutureUtils.combine(
        updatableTopicPartitions.thenCompose(this::earliestOffsets),
        updatableTopicPartitions.thenCompose(this::latestOffsets),
        updatableTopicPartitions
            .thenCompose(this::maxTimestamps)
            // supported version: 3.0.0
            // https://issues.apache.org/jira/browse/KAFKA-12541
            // It is fine to return partition without max timestamp
            .exceptionally(exceptionHandler(UnsupportedVersionException.class, Map.of())),
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
        brokers()
            .thenApply(
                brokers ->
                    brokers.stream().collect(Collectors.toMap(Broker::id, broker -> broker))),
        (earliestOffsets, latestOffsets, maxTimestamps, tpInfos, topicAndInternal, brokers) ->
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
                              : tpInfo.leader().id();
                      var replicas =
                          tpInfo.replicas().stream()
                              .map(node -> brokers.getOrDefault(node.id(), Broker.of(node)))
                              .collect(Collectors.toList());
                      var isr =
                          tpInfo.isr().stream()
                              .map(node -> brokers.getOrDefault(node.id(), Broker.of(node)))
                              .collect(Collectors.toList());
                      return new Partition(
                          tp.topic(),
                          tp.partition(),
                          earliest,
                          latest,
                          maxTimestamp,
                          Optional.ofNullable(leader),
                          replicas,
                          isr,
                          topicAndInternal.get(tp.topic()));
                    })
                .sorted(Comparator.comparing(Partition::topicPartition))
                .collect(Collectors.toList()));
  }

  @Override
  public CompletionStage<List<Broker>> brokers() {
    return clusterIdAndBrokers().thenApply(Map.Entry::getValue);
  }

  @Override
  public CompletionStage<List<Controller>> controllers() {
    return to(admin(true).describeCluster().nodes())
        .thenCompose(
            nodes ->
                to(admin(true).describeMetadataQuorum().quorumInfo())
                    .thenCompose(
                        quorumInfo ->
                            to(controllerAdmin
                                    .describeConfigs(
                                        nodes.stream()
                                            // don't query the died nodes
                                            .filter(
                                                n ->
                                                    quorumInfo.voters().stream()
                                                            .anyMatch(
                                                                r ->
                                                                    r.replicaId() == n.id()
                                                                        && r.lastCaughtUpTimestamp()
                                                                            .isPresent())
                                                        || quorumInfo.observers().stream()
                                                            .anyMatch(
                                                                r ->
                                                                    r.replicaId() == n.id()
                                                                        && r.lastCaughtUpTimestamp()
                                                                            .isPresent()))
                                            .map(
                                                n ->
                                                    new ConfigResource(
                                                        ConfigResource.Type.BROKER,
                                                        String.valueOf(n.id())))
                                            .toList())
                                    .all())
                                .thenApply(
                                    configs ->
                                        nodes.stream()
                                            .map(
                                                n ->
                                                    new Controller(
                                                        n.id(),
                                                        n.host(),
                                                        n.port(),
                                                        new org.astraea.common.admin.Config(
                                                            configs
                                                                .getOrDefault(
                                                                    new ConfigResource(
                                                                        ConfigResource.Type.BROKER,
                                                                        String.valueOf(n.id())),
                                                                    new Config(List.of()))
                                                                .entries()
                                                                .stream()
                                                                .filter(
                                                                    entry ->
                                                                        entry.value() != null
                                                                            && !entry
                                                                                .value()
                                                                                .isBlank())
                                                                .collect(
                                                                    Collectors.toMap(
                                                                        ConfigEntry::name,
                                                                        ConfigEntry::value)))))
                                            .toList())));
  }

  private CompletionStage<Map.Entry<String, List<Broker>>> clusterIdAndBrokers() {
    var cluster = admin(false).describeCluster();
    var nodeFuture = to(cluster.nodes());
    return FutureUtils.combine(
        to(cluster.clusterId()),
        to(cluster.controller()),
        nodeFuture.thenCompose(
            nodes ->
                to(
                    kafkaAdmin
                        .describeLogDirs(nodes.stream().map(Node::id).collect(Collectors.toList()))
                        .allDescriptions())),
        nodeFuture.thenCompose(
            nodes ->
                doGetConfigs(
                    nodes.stream()
                        .map(
                            n ->
                                new ConfigResource(
                                    ConfigResource.Type.BROKER, String.valueOf(n.id())))
                        .collect(Collectors.toList()),
                    false)),
        nodeFuture,
        (id, controller, logDirs, configs, nodes) ->
            Map.entry(
                id,
                nodes.stream()
                    .map(
                        node ->
                            Broker.of(
                                node.id() == controller.id(),
                                node,
                                configs.get(String.valueOf(node.id())),
                                logDirs.get(node.id())))
                    .sorted(Comparator.comparing(Broker::id))
                    .collect(Collectors.toList())));
  }

  @Override
  public CompletionStage<List<ShareGroup>> shareGroups(Set<String> shareGroupIds) {
    if (shareGroupIds.isEmpty()) return CompletableFuture.completedFuture(List.of());
    return FutureUtils.combine(
        to(admin(false).describeShareGroups(shareGroupIds).all()),
        kafkaAdmin
            .listShareGroupOffsets(
                shareGroupIds.stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Function.identity(), __ -> new ListShareGroupOffsetsSpec())))
            .all()
            .toCompletionStage(),
        (shareGroupDescriptions, shareGroupMetadata) ->
            shareGroupDescriptions.entrySet().stream()
                .map(
                    g ->
                        new ShareGroup(
                            g.getKey(),
                            g.getValue().groupState().toString(),
                            g.getValue().coordinator().id(),
                            g.getValue().groupEpoch(),
                            g.getValue().targetAssignmentEpoch(),
                            shareGroupMetadata.get(g.getKey()).entrySet().stream()
                                .collect(
                                    Collectors.toUnmodifiableMap(
                                        tp -> TopicPartition.from(tp.getKey()),
                                        offset -> offset.getValue().offset())),
                            g.getValue().members().stream()
                                .collect(
                                    Collectors.toMap(
                                        m ->
                                            new ShareMember(
                                                g.getKey(),
                                                m.consumerId(),
                                                m.clientId(),
                                                m.memberEpoch(),
                                                m.host()),
                                        m ->
                                            m.assignment().topicPartitions().stream()
                                                .map(TopicPartition::from)
                                                .collect(Collectors.toUnmodifiableSet())))))
                .toList());
  }

  @Override
  public CompletionStage<Map<GroupType, Set<String>>> groupIds() {
    return to(admin(false).listGroups().all())
        .thenApply(
            gs ->
                gs.stream()
                    .filter(g -> g.type().isPresent())
                    .collect(
                        Collectors.groupingBy(
                            g -> GroupType.of(g.type().get()),
                            Collectors.mapping(
                                GroupListing::groupId, Collectors.toUnmodifiableSet()))));
  }

  @Override
  public CompletionStage<List<ConsumerGroup>> consumerGroups(Set<String> consumerGroupIds) {
    if (consumerGroupIds.isEmpty()) return CompletableFuture.completedFuture(List.of());
    return FutureUtils.combine(
        to(admin(false).describeConsumerGroups(consumerGroupIds).all()),
        kafkaAdmin
            .listConsumerGroupOffsets(
                consumerGroupIds.stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(), __ -> new ListConsumerGroupOffsetsSpec())))
            .all()
            .toCompletionStage(),
        (consumerGroupDescriptions, consumerGroupMetadata) ->
            consumerGroupIds.stream()
                .map(
                    groupId ->
                        new ConsumerGroup(
                            groupId,
                            consumerGroupDescriptions.get(groupId).partitionAssignor(),
                            consumerGroupDescriptions.get(groupId).groupState().name(),
                            consumerGroupDescriptions.get(groupId).type().toString(),
                            consumerGroupDescriptions.get(groupId).coordinator().id(),
                            consumerGroupMetadata.get(groupId).entrySet().stream()
                                .collect(
                                    Collectors.toUnmodifiableMap(
                                        tp -> TopicPartition.from(tp.getKey()),
                                        offset -> offset.getValue().offset())),
                            consumerGroupDescriptions.get(groupId).members().stream()
                                .collect(
                                    Collectors.toUnmodifiableMap(
                                        member ->
                                            new ConsumerMember(
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
            .allTopicNames())
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
                            availablePartitions.stream().map(TopicPartition::to).toList())
                        .all())
                    // supported version: 2.8.0
                    // https://issues.apache.org/jira/browse/KAFKA-12238
                    .exceptionally(exceptionHandler(UnsupportedVersionException.class, Map.of()))
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
    return to(admin(false).listTransactions().all())
        .thenApply(
            t ->
                t.stream()
                    .map(TransactionListing::transactionalId)
                    .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletionStage<List<Transaction>> transactions(Set<String> transactionIds) {
    if (transactionIds.isEmpty()) return CompletableFuture.completedFuture(List.of());
    return to(admin(false).describeTransactions(transactionIds).all())
        .thenApply(
            ts ->
                ts.entrySet().stream()
                    .map(e -> Transaction.of(e.getKey(), e.getValue()))
                    .sorted(Comparator.comparing(Transaction::transactionId))
                    .toList());
  }

  @Override
  public CompletionStage<ClusterInfo> clusterInfo(Set<String> topics) {
    return FutureUtils.combine(
        clusterIdAndBrokers(),
        topics(topics),
        replicas(topics),
        (clusterIdAndBrokers, topicList, replicas) -> {
          var topicMap =
              topicList.stream().collect(Collectors.toUnmodifiableMap(Topic::name, t -> t));
          return ClusterInfo.of(
              clusterIdAndBrokers.getKey(), clusterIdAndBrokers.getValue(), topicMap, replicas);
        });
  }

  private CompletionStage<List<Replica>> replicas(Set<String> topics) {
    if (topics.isEmpty()) return CompletableFuture.completedFuture(List.of());

    // pre-group folders by (broker -> topic partition) to speedup seek
    return FutureUtils.combine(
        logDirs(),
        to(admin(false).describeTopics(topics).allTopicNames()),
        to(admin(false).listPartitionReassignments().reassignments())
            // supported version: 2.4.0
            // https://issues.apache.org/jira/browse/KAFKA-8345
            .exceptionally(exceptionHandler(UnsupportedVersionException.class, Map.of())),
        brokers()
            .thenApply(
                brokers ->
                    brokers.stream().collect(Collectors.toMap(Broker::id, broker -> broker))),
        (logDirs, ts, reassignmentMap, brokers) ->
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
                                                  .isInternal(internal)
                                                  .isAdding(isAdding)
                                                  .isRemoving(isRemoving)
                                                  .brokerId(
                                                      brokers
                                                          .getOrDefault(node.id(), Broker.of(node))
                                                          .id())
                                                  .lag(pathAndReplica.getValue().offsetLag())
                                                  .size(pathAndReplica.getValue().size())
                                                  .isLeader(
                                                      partition.leader() != null
                                                          && !partition.leader().isEmpty()
                                                          && partition.leader().id() == node.id())
                                                  .isSync(partition.isr().contains(node))
                                                  .isFuture(pathAndReplica.getValue().isFuture())
                                                  .isOffline(
                                                      node.isEmpty()
                                                          || pathAndReplica.getKey().isEmpty())
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
                        .thenComparing(Replica::brokerId))
                .toList());
  }

  @Override
  public CompletionStage<List<Quota>> quotas(Map<String, Set<String>> targets) {
    return quotas(
            ClientQuotaFilter.contains(
                targets.entrySet().stream()
                    .flatMap(
                        t ->
                            t.getValue().stream()
                                .map(v -> ClientQuotaFilterComponent.ofEntity(t.getKey(), v)))
                    .collect(Collectors.toList())))
        .thenApply(Quota::of);
  }

  @Override
  public CompletionStage<List<Quota>> quotas(Set<String> targetKeys) {
    return quotas(
            ClientQuotaFilter.contains(
                targetKeys.stream()
                    .map(ClientQuotaFilterComponent::ofEntityType)
                    .collect(Collectors.toList())))
        .thenApply(Quota::of);
  }

  @Override
  public CompletionStage<List<Quota>> quotas() {
    return quotas(ClientQuotaFilter.all()).thenApply(Quota::of);
  }

  private CompletionStage<Map<ClientQuotaEntity, Map<String, Double>>> quotas(
      ClientQuotaFilter filter) {
    return to(admin(false).describeClientQuotas(filter).entities());
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

  private static Map<String, String> entity(String name) {
    var map = new HashMap<String, String>();
    map.put(name, null);
    return map;
  }

  @Override
  public CompletionStage<Void> setConnectionQuota(int rate) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                List.of(
                    new org.apache.kafka.common.quota.ClientQuotaAlteration(
                        new ClientQuotaEntity(entity(ClientQuotaEntity.IP)),
                        List.of(
                            new ClientQuotaAlteration.Op(
                                QuotaConfigs.IP_CONNECTION_RATE_CONFIG, (double) rate)))))
            .all());
  }

  @Override
  public CompletionStage<QuorumInfo> quorumInfo() {
    return to(admin(false).describeMetadataQuorum().quorumInfo())
        .thenApply(
            quorumInfo ->
                new QuorumInfo(
                    quorumInfo.leaderId(),
                    quorumInfo.leaderEpoch(),
                    quorumInfo.highWatermark(),
                    quorumInfo.voters().stream()
                        .map(
                            v ->
                                new ReplicaState(
                                    v.replicaId(),
                                    v.replicaDirectoryId(),
                                    v.logEndOffset(),
                                    v.lastFetchTimestamp(),
                                    v.lastCaughtUpTimestamp()))
                        .toList(),
                    quorumInfo.observers().stream()
                        .map(
                            v ->
                                new ReplicaState(
                                    v.replicaId(),
                                    v.replicaDirectoryId(),
                                    v.logEndOffset(),
                                    v.lastFetchTimestamp(),
                                    v.lastCaughtUpTimestamp()))
                        .toList(),
                    quorumInfo.nodes().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                    e.getValue().endpoints().stream()
                                        .map(
                                            p -> new RaftEndpoint(p.listener(), p.host(), p.port()))
                                        .toList()))));
  }

  @Override
  public CompletionStage<Void> addVoter(
      int nodeId, String directoryId, List<RaftEndpoint> endpoints) {
    return to(
        kafkaAdmin
            .addRaftVoter(
                nodeId,
                Uuid.fromString(directoryId),
                endpoints.stream()
                    .map(e -> new RaftVoterEndpoint(e.name(), e.host(), e.port()))
                    .collect(Collectors.toSet()))
            .all());
  }

  @Override
  public CompletionStage<Void> removeVoter(int nodeId, String directoryId) {
    return to(admin(false).removeRaftVoter(nodeId, Uuid.fromString(directoryId)).all());
  }

  @Override
  public CompletionStage<Void> unsetConnectionQuotas(Set<String> ips) {
    if (ips.isEmpty()) {
      return to(
          kafkaAdmin
              .alterClientQuotas(
                  List.of(
                      new ClientQuotaAlteration(
                          new ClientQuotaEntity(entity(ClientQuotaEntity.IP)),
                          List.of(
                              new ClientQuotaAlteration.Op(
                                  QuotaConfigs.IP_CONNECTION_RATE_CONFIG, null)))))
              .all());
    }
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
  public CompletionStage<Void> setConsumerQuota(DataRate rate) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                List.of(
                    new ClientQuotaAlteration(
                        new ClientQuotaEntity(entity(ClientQuotaEntity.CLIENT_ID)),
                        List.of(
                            new ClientQuotaAlteration.Op(
                                QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG, rate.byteRate())))))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetConsumerQuotas(Set<String> clientIds) {
    if (clientIds.isEmpty()) {
      return to(
          kafkaAdmin
              .alterClientQuotas(
                  List.of(
                      new ClientQuotaAlteration(
                          new ClientQuotaEntity(entity(ClientQuotaEntity.CLIENT_ID)),
                          List.of(
                              new ClientQuotaAlteration.Op(
                                  QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG, null)))))
              .all());
    }
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
  public CompletionStage<Void> setProducerQuota(DataRate rate) {
    return to(
        kafkaAdmin
            .alterClientQuotas(
                List.of(
                    new ClientQuotaAlteration(
                        new ClientQuotaEntity(entity(ClientQuotaEntity.CLIENT_ID)),
                        List.of(
                            new ClientQuotaAlteration.Op(
                                QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG, rate.byteRate())))))
            .all());
  }

  @Override
  public CompletionStage<Void> unsetProducerQuotas(Set<String> clientIds) {
    if (clientIds.isEmpty()) {
      return to(
          kafkaAdmin
              .alterClientQuotas(
                  List.of(
                      new ClientQuotaAlteration(
                          new ClientQuotaEntity(entity(ClientQuotaEntity.CLIENT_ID)),
                          List.of(
                              new ClientQuotaAlteration.Op(
                                  QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG, null)))))
              .all());
    }
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
      private Map<Integer, List<Integer>> replicasAssignments = null;
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
      public TopicCreator replicasAssignments(Map<Integer, List<Integer>> replicasAssignments) {
        this.replicasAssignments = Objects.requireNonNull(replicasAssignments);
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
        if ((numberOfPartitions != 1 || numberOfReplicas != 1) && replicasAssignments != null)
          throw new IllegalArgumentException(
              "Using \"numberOfPartitions\" and \"numberOfReplicas\" settings "
                  + "is mutually exclusive with specifying \"replicasAssignments\"");

        return topicNames(true)
            .thenCompose(
                names -> {
                  if (!names.contains(topic))
                    return to(kafkaAdmin
                            .createTopics(
                                List.of(
                                    replicasAssignments == null
                                        ? new NewTopic(topic, numberOfPartitions, numberOfReplicas)
                                            .configs(configs)
                                        : new NewTopic(topic, replicasAssignments)
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
  public CompletionStage<Void> declarePreferredDataFolders(
      Map<TopicPartitionReplica, String> assignments) {
    if (assignments.isEmpty()) return CompletableFuture.completedFuture(null);
    return clusterInfo(
            assignments.keySet().stream()
                .map(TopicPartitionReplica::topic)
                .collect(Collectors.toUnmodifiableSet()))
        .thenApply(
            cluster ->
                assignments.entrySet().stream()
                    .filter(e -> cluster.replicas(e.getKey()).isEmpty())
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .thenCompose(
            preferredAssignments ->
                preferredAssignments.isEmpty()
                    ? CompletableFuture.completedStage(null)
                    : this.moveToFolders(preferredAssignments)
                        .handle(
                            (r, e) -> {
                              if (e == null)
                                throw new RuntimeException(
                                    "Fail to expect a ReplicaNotAvailableException return from the API. "
                                        + "A data folder movement might just triggered. "
                                        + "Is there another Admin Client manipulating the cluster state?");
                              if (e instanceof ReplicaNotAvailableException) return null;
                              throw (RuntimeException) e;
                            }));
  }

  @Override
  public CompletionStage<Void> preferredLeaderElection(Set<TopicPartition> partitions) {
    return to(kafkaAdmin
            .electLeaders(
                ElectionType.PREFERRED,
                partitions.stream().map(TopicPartition::to).collect(Collectors.toSet()))
            .all())
        // This error occurred if the preferred leader of the given topic/partition is already
        // the leader. It is ok to swallow the exception since the preferred leader be the
        // actual leader. That is what the caller wants to be.
        .exceptionally(exceptionHandler(ElectionNotNeededException.class, null));
  }

  @Override
  public CompletionStage<Void> addPartitions(String topic, int total) {
    return to(admin(false).createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
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

  @Override
  public CompletionStage<Void> setClusterConfigs(Map<String, String> override) {
    return doSetConfigs(Map.of(new ConfigResource(ConfigResource.Type.BROKER, ""), override));
  }

  @Override
  public CompletionStage<org.astraea.common.admin.Config> clusterConfigs() {
    return to(kafkaAdmin
            .describeConfigs(List.of(new ConfigResource(ConfigResource.Type.BROKER, "")))
            .all())
        .thenApply(
            configs ->
                new org.astraea.common.admin.Config(
                    configs.values().stream()
                        .flatMap(s -> s.entries().stream())
                        .filter(
                            s ->
                                s.source()
                                    == ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)
                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))));
  }

  private CompletionStage<Map<String, Map<String, String>>> doGetConfigs(
      Collection<ConfigResource> resources, boolean fromController) {
    return to(admin(fromController).describeConfigs(resources).all())
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

    return doGetConfigs(nonEmptyAppend.keySet(), false)
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
                      ignored -> to(admin(false).incrementalAlterConfigs(requestToAppend).all()));
            });
  }

  private CompletionStage<Void> doSubtractConfigs(
      Map<ConfigResource, Map<String, String>> subtract) {

    var nonEmptySubtract =
        subtract.entrySet().stream()
            .filter(r -> !r.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (nonEmptySubtract.isEmpty()) return CompletableFuture.completedFuture(null);

    return doGetConfigs(nonEmptySubtract.keySet(), false)
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
                                                    .toList();
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
              return to(admin(false).incrementalAlterConfigs(requestToSubtract).all());
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
    return to(admin(false).incrementalAlterConfigs(newVersion.get()).all());
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

    return to(admin(false).incrementalAlterConfigs(newVersion.get()).all());
  }

  @Override
  public void close() {
    admin(false).close();
    controllerAdmin.close();
  }

  private CompletionStage<
          Map<
              Integer,
              Map<TopicPartition, Map<String, org.apache.kafka.clients.admin.ReplicaInfo>>>>
      logDirs() {
    return brokers()
        .thenApply(
            brokers -> brokers.stream().map(Broker::id).collect(Collectors.toUnmodifiableSet()))
        .thenCompose(ids -> to(admin(false).describeLogDirs(ids).allDescriptions()))
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

  private <T> Function<Throwable, T> exceptionHandler(
      Class<? extends Exception> clz, T defaultValue) {
    return e -> {
      if (clz.isInstance(e) || (e.getCause() != null && clz.isInstance(e.getCause())))
        return defaultValue;

      if (e instanceof RuntimeException) throw (RuntimeException) e;
      throw new RuntimeException(e);
    };
  }
}
