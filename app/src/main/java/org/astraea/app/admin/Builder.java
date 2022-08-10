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
package org.astraea.app.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;

public class Builder {

  private final Map<String, Object> configs = new HashMap<>();
  private static final String ERROR_MSG_MEMBER_IS_EMPTY = "leaving members should not be empty";

  Builder() {}

  public Builder bootstrapServers(String bootstrapServers) {
    this.configs.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
    return this;
  }

  public Builder configs(Map<String, String> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Admin build() {
    return new AdminImpl(org.apache.kafka.clients.admin.Admin.create(configs));
  }

  private static class AdminImpl implements Admin {
    private final org.apache.kafka.clients.admin.Admin admin;

    AdminImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = Objects.requireNonNull(admin);
    }

    @Override
    public void close() {
      admin.close();
    }

    @Override
    public Set<NodeInfo> nodes() {
      return Utils.packException(
          () ->
              admin.describeCluster().nodes().get().stream()
                  .map(NodeInfo::of)
                  .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public Map<Integer, Set<String>> brokerFolders(Set<Integer> brokers) {
      return Utils.packException(
          () ->
              admin.describeLogDirs(brokers).allDescriptions().get().entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, map -> map.getValue().keySet())));
    }

    @Override
    public ReplicaMigrator migrator() {
      return new MigratorImpl(admin, this::partitions);
    }

    @Override
    public void preferredLeaderElection(TopicPartition topicPartition) {
      try {
        Utils.packException(
            () -> {
              admin
                  .electLeaders(ElectionType.PREFERRED, Set.of(TopicPartition.to(topicPartition)))
                  .all()
                  .get();
            });
      } catch (ElectionNotNeededException ignored) {
        // Swallow the ElectionNotNeededException.
        // This error occurred if the preferred leader of the given topic/partition is already the
        // leader. It is ok to swallow the exception since the preferred leader be the actual
        // leader. That is what the caller wants to be.
      }
    }

    @Override
    public Map<TopicPartition, Collection<ProducerState>> producerStates(
        Set<TopicPartition> partitions) {
      return Utils.packException(
          () ->
              admin
                  .describeProducers(
                      partitions.stream()
                          .map(TopicPartition::to)
                          .collect(Collectors.toUnmodifiableList()))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .filter(e -> !e.getValue().activeProducers().isEmpty())
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()),
                          e ->
                              e.getValue().activeProducers().stream()
                                  .map(ProducerState::from)
                                  .collect(Collectors.toUnmodifiableList()))));
    }

    @Override
    public Set<String> consumerGroupIds() {
      return Utils.packException(() -> admin.listConsumerGroups().all().get()).stream()
          .map(ConsumerGroupListing::groupId)
          .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Map<String, ConsumerGroup> consumerGroups(Set<String> consumerGroupNames) {
      return Utils.packException(
          () -> {
            var consumerGroupDescriptions =
                admin.describeConsumerGroups(consumerGroupNames).all().get();

            var consumerGroupMetadata =
                consumerGroupNames.stream()
                    .map(x -> Map.entry(x, admin.listConsumerGroupOffsets(x)))
                    .map(
                        x ->
                            Map.entry(
                                x.getKey(),
                                Utils.packException(
                                    () -> x.getValue().partitionsToOffsetAndMetadata().get())))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            var createMember =
                (BiFunction<String, MemberDescription, Member>)
                    (s, x) ->
                        new Member(s, x.consumerId(), x.groupInstanceId(), x.clientId(), x.host());

            return consumerGroupNames.stream()
                .map(
                    groupId -> {
                      var members =
                          consumerGroupDescriptions.get(groupId).members().stream()
                              .map(x -> createMember.apply(groupId, x))
                              .collect(Collectors.toUnmodifiableList());
                      var consumeOffset =
                          consumerGroupMetadata.get(groupId).entrySet().stream()
                              .collect(
                                  Collectors.toUnmodifiableMap(
                                      tp -> TopicPartition.from(tp.getKey()),
                                      x -> x.getValue().offset()));
                      var assignment =
                          consumerGroupDescriptions.get(groupId).members().stream()
                              .collect(
                                  Collectors.toUnmodifiableMap(
                                      x -> createMember.apply(groupId, x),
                                      x ->
                                          x.assignment().topicPartitions().stream()
                                              .map(TopicPartition::from)
                                              .collect(Collectors.toSet())));

                      return Map.entry(
                          groupId, new ConsumerGroup(groupId, members, consumeOffset, assignment));
                    })
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
          });
    }

    private Map<TopicPartition, Long> earliestOffset(Set<TopicPartition> partitions) {

      return Utils.packException(
          () ->
              admin
                  .listOffsets(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to, e -> new OffsetSpec.EarliestSpec())))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()), e -> e.getValue().offset())));
    }

    private Map<TopicPartition, Long> latestOffset(Set<TopicPartition> partitions) {
      return Utils.packException(
          () ->
              admin
                  .listOffsets(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to, e -> new OffsetSpec.LatestSpec())))
                  .all()
                  .get()
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          e -> TopicPartition.from(e.getKey()), e -> e.getValue().offset())));
    }

    @Override
    public Map<String, Config> topics(Set<String> topicNames) {
      return Utils.packException(
              () ->
                  admin
                      .describeConfigs(
                          topicNames.stream()
                              .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(Collectors.toMap(e -> e.getKey().name(), e -> new ConfigImpl(e.getValue())));
    }

    @Override
    public Set<String> topicNames() {
      return Utils.packException(
          () -> admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get());
    }

    @Override
    public void deleteTopics(Set<String> topicNames) {
      Utils.packException(() -> admin.deleteTopics(topicNames).all().get());
    }

    @Override
    public Map<Integer, Config> brokers(Set<Integer> brokerIds) {
      return Utils.packException(
              () ->
                  admin
                      .describeConfigs(
                          brokerIds.stream()
                              .map(
                                  id ->
                                      new ConfigResource(
                                          ConfigResource.Type.BROKER, String.valueOf(id)))
                              .collect(Collectors.toList()))
                      .all()
                      .get())
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  e -> Integer.valueOf(e.getKey().name()), e -> new ConfigImpl(e.getValue())));
    }

    @Override
    public Map<TopicPartition, Offset> offsets(Set<String> topics) {
      var partitions = partitions(topics);
      var earliest = earliestOffset(partitions);
      var latest = latestOffset(partitions);
      return earliest.entrySet().stream()
          .filter(e -> latest.containsKey(e.getKey()))
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, e -> new Offset(e.getValue(), latest.get(e.getKey()))));
    }

    @Override
    public Set<TopicPartition> partitions(Set<String> topics) {
      return Utils.packException(
          () ->
              admin.describeTopics(topics).all().get().entrySet().stream()
                  .flatMap(
                      e ->
                          e.getValue().partitions().stream()
                              .map(p -> TopicPartition.of(e.getKey(), p.partition())))
                  .collect(Collectors.toSet()));
    }

    @Override
    public Map<Integer, Set<TopicPartition>> partitions(
        Set<String> topics, Set<Integer> brokerIds) {
      return replicas(topics).entrySet().stream()
          .flatMap(
              e -> e.getValue().stream().map(replica -> Map.entry(replica.broker(), e.getKey())))
          .filter(e -> brokerIds.contains(e.getKey()))
          .collect(Collectors.groupingBy(Map.Entry::getKey))
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> e.getValue().stream().map(Map.Entry::getValue).collect(Collectors.toSet())));
    }

    @Override
    public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
      return Utils.packException(
          () -> {
            var logInfo = admin.describeLogDirs(brokerIds()).allDescriptions().get();
            var tpPathMap = new HashMap<TopicPartition, Map<Integer, String>>();
            for (var entry0 : logInfo.entrySet()) {
              for (var entry1 : entry0.getValue().entrySet()) {
                for (var entry2 : entry1.getValue().replicaInfos().entrySet()) {
                  var tp = TopicPartition.from(entry2.getKey());
                  var broker = entry0.getKey();
                  var dataPath = entry1.getKey();
                  tpPathMap.computeIfAbsent(tp, (ignore) -> new HashMap<>()).put(broker, dataPath);
                }
              }
            }
            return admin.describeTopics(topics).allTopicNames().get().entrySet().stream()
                .flatMap(
                    entry ->
                        entry.getValue().partitions().stream()
                            .map(
                                tpInfo -> {
                                  var topicName = entry.getKey();
                                  var partition = tpInfo.partition();
                                  var topicPartition = TopicPartition.of(topicName, partition);
                                  return Map.entry(topicPartition, tpInfo);
                                }))
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        (entry) -> {
                          var topicPartition = entry.getKey();
                          var tpInfo = entry.getValue();
                          var replicaLeaderId = tpInfo.leader() != null ? tpInfo.leader().id() : -1;
                          var isrSet = tpInfo.isr();
                          // The first replica in the return result is the preferred leader. This
                          // only works with Kafka broker version after 0.11. Version before 0.11
                          // returns the replicas in unspecified order due to a bug.
                          var preferredLeader = entry.getValue().replicas().get(0);
                          return entry.getValue().replicas().stream()
                              .map(
                                  node -> {
                                    int broker = node.id();
                                    var dataPath =
                                        tpPathMap
                                            .getOrDefault(topicPartition, Map.of())
                                            .getOrDefault(broker, null);
                                    var replicaInfo =
                                        node.isEmpty() || dataPath == null
                                            ? null
                                            : logInfo
                                                .get(broker)
                                                .get(dataPath)
                                                .replicaInfos()
                                                .get(TopicPartition.to(topicPartition));
                                    boolean isLeader = node.id() == replicaLeaderId;
                                    boolean inSync = isrSet.contains(node);
                                    long lag = replicaInfo != null ? replicaInfo.offsetLag() : -1L;
                                    long size = replicaInfo != null ? replicaInfo.size() : -1L;
                                    boolean future = replicaInfo != null && replicaInfo.isFuture();
                                    boolean offline = node.isEmpty();
                                    boolean isPreferredLeader = preferredLeader.id() == broker;
                                    return new Replica(
                                        broker,
                                        lag,
                                        size,
                                        isLeader,
                                        inSync,
                                        future,
                                        offline,
                                        isPreferredLeader,
                                        dataPath);
                                  })
                              .collect(Collectors.toList());
                        }));
          });
    }

    @Override
    public TopicCreator creator() {
      return new CreatorImpl(
          admin, topic -> this.replicas(Set.of(topic)), topic -> topics().get(topic));
    }

    @Override
    public QuotaCreator quotaCreator() {
      return new QuotaImpl(admin);
    }

    @Override
    public Collection<Quota> quotas(Quota.Target target) {
      return quotas(
          ClientQuotaFilter.contains(
              List.of(ClientQuotaFilterComponent.ofEntityType(target.nameOfKafka()))));
    }

    @Override
    public Collection<Quota> quotas(Quota.Target target, String value) {
      return quotas(
          ClientQuotaFilter.contains(
              List.of(ClientQuotaFilterComponent.ofEntity(target.nameOfKafka(), value))));
    }

    @Override
    public Collection<Quota> quotas() {
      return quotas(ClientQuotaFilter.all());
    }

    private Collection<Quota> quotas(ClientQuotaFilter filter) {
      return Quota.of(
          Utils.packException(() -> admin.describeClientQuotas(filter).entities().get()));
    }

    @Override
    public ClusterInfo clusterInfo(Set<String> topics) {
      final var nodeInfo = this.nodes().stream().collect(Collectors.toUnmodifiableList());

      final Map<String, List<ReplicaInfo>> topicToReplicasMap =
          Utils.packException(() -> this.replicas(topics)).entrySet().stream()
              .flatMap(
                  entry -> {
                    final var topicPartition = entry.getKey();
                    final var replicas = entry.getValue();

                    return replicas.stream()
                        .map(
                            replica ->
                                ReplicaInfo.of(
                                    topicPartition.topic(),
                                    topicPartition.partition(),
                                    nodeInfo.stream()
                                        .filter(x -> x.id() == replica.broker())
                                        .findFirst()
                                        .orElse(NodeInfo.ofOfflineNode(replica.broker())),
                                    replica.leader(),
                                    replica.inSync(),
                                    replica.isOffline(),
                                    replica.path()));
                  })
              .collect(Collectors.groupingBy(ReplicaInfo::topic));
      final var dataDirectories =
          this.brokerFolders(
                  nodeInfo.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()))
              .entrySet()
              .stream()
              .collect(
                  Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> Set.copyOf(x.getValue())));

      return new ClusterInfo() {
        @Override
        public List<NodeInfo> nodes() {
          return nodeInfo;
        }

        @Override
        public Set<String> dataDirectories(int brokerId) {
          final var set = dataDirectories.get(brokerId);
          if (set == null) throw new NoSuchElementException("Unknown broker \"" + brokerId + "\"");
          else return set;
        }

        @Override
        public List<ReplicaInfo> availableReplicaLeaders(String topic) {
          return replicas(topic).stream()
              .filter(ReplicaInfo::isLeader)
              .filter(ReplicaInfo::isOnlineReplica)
              .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public List<ReplicaInfo> availableReplicas(String topic) {
          return replicas(topic).stream()
              .filter(ReplicaInfo::isOnlineReplica)
              .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public Set<String> topics() {
          return topics;
        }

        @Override
        public List<ReplicaInfo> replicas(String topic) {
          final var replicaInfos = topicToReplicasMap.get(topic);
          if (replicaInfos == null)
            throw new NoSuchElementException(
                "This ClusterInfo have no information about topic \"" + topic + "\"");
          else return replicaInfos;
        }
      };
    }

    @Override
    public Set<String> transactionIds() {
      return Utils.packException(
          () ->
              admin.listTransactions().all().get().stream()
                  .map(TransactionListing::transactionalId)
                  .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public Map<String, Transaction> transactions(Set<String> transactionIds) {
      return Utils.packException(
          () ->
              admin.describeTransactions(transactionIds).all().get().entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, e -> Transaction.from(e.getValue()))));
    }

    @Override
    public void removeGroup(String groupId) {
      Utils.packException(() -> admin.deleteConsumerGroups(Set.of(groupId)).all().get());
    }

    @Override
    public void removeAllMembers(String groupId) {
      try {
        Utils.packException(
            () -> {
              admin
                  .removeMembersFromConsumerGroup(
                      groupId, new RemoveMembersFromConsumerGroupOptions())
                  .all()
                  .get();
            });
      } catch (IllegalArgumentException e) {
        // Deleting all members can't work when there is no members already.
        if (!ERROR_MSG_MEMBER_IS_EMPTY.equals(e.getMessage())) {
          throw e;
        }
      }
    }

    @Override
    public void removeStaticMembers(String groupId, Set<String> members) {
      Utils.packException(
          () ->
              admin
                  .removeMembersFromConsumerGroup(
                      groupId,
                      new RemoveMembersFromConsumerGroupOptions(
                          members.stream()
                              .map(MemberToRemove::new)
                              .collect(Collectors.toUnmodifiableList())))
                  .all()
                  .get());
    }

    @Override
    public Map<TopicPartition, Reassignment> reassignments(Set<String> topics) {
      var assignments =
          Utils.packException(
              () ->
                  admin
                      .listPartitionReassignments(
                          partitions(topics).stream()
                              .map(TopicPartition::to)
                              .collect(Collectors.toSet()))
                      .reassignments()
                      .get());

      var dirs =
          Utils.packException(
              () ->
                  admin
                      .describeReplicaLogDirs(
                          replicas(topics).entrySet().stream()
                              .flatMap(
                                  e ->
                                      e.getValue().stream()
                                          .map(
                                              r ->
                                                  new org.apache.kafka.common.TopicPartitionReplica(
                                                      e.getKey().topic(),
                                                      e.getKey().partition(),
                                                      r.broker())))
                              .collect(Collectors.toUnmodifiableList()))
                      .all()
                      .get());

      var result =
          new HashMap<
              TopicPartition, Map.Entry<Set<Reassignment.Location>, Set<Reassignment.Location>>>();

      dirs.forEach(
          (replica, logDir) -> {
            var brokerId = replica.brokerId();
            var tp = TopicPartition.of(replica.topic(), replica.partition());
            var ls =
                result.computeIfAbsent(tp, ignored -> Map.entry(new HashSet<>(), new HashSet<>()));
            // the replica is moved from a folder to another folder (in the same node)
            if (logDir.getFutureReplicaLogDir() != null)
              ls.getValue()
                  .add(new Reassignment.Location(brokerId, logDir.getFutureReplicaLogDir()));
            if (logDir.getCurrentReplicaLogDir() != null) {
              var assignment = assignments.get(TopicPartition.to(tp));
              // the replica is moved from a node to another node
              if (assignment != null && assignment.addingReplicas().contains(brokerId)) {
                ls.getValue()
                    .add(new Reassignment.Location(brokerId, logDir.getCurrentReplicaLogDir()));
              } else {
                ls.getKey()
                    .add(new Reassignment.Location(brokerId, logDir.getCurrentReplicaLogDir()));
              }
            }
          });

      return result.entrySet().stream()
          // empty "to" means there is no reassignment
          .filter(e -> !e.getValue().getValue().isEmpty())
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e ->
                      new Reassignment(
                          Collections.unmodifiableSet(e.getValue().getKey()),
                          Collections.unmodifiableSet(e.getValue().getValue()))));
    }

    @Override
    public Map<TopicPartition, DeletedRecord> deleteRecords(
        Map<TopicPartition, Long> recordsToDelete) {
      var kafkaRecordsToDelete =
          recordsToDelete.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      x -> TopicPartition.to(x.getKey()),
                      x -> RecordsToDelete.beforeOffset(x.getValue())));
      return admin.deleteRecords(kafkaRecordsToDelete).lowWatermarks().entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  x -> TopicPartition.from(x.getKey()),
                  x -> DeletedRecord.from(Utils.packException(() -> x.getValue().get()))));
    }

    @Override
    public ReplicationThrottler replicationThrottler() {
      return new ReplicationThrottlerImpl(admin);
    }

    @Override
    public void clearReplicationThrottle(String topic) {
      var configEntry0 = new ConfigEntry(ReplicationThrottlerImpl.THROTTLE_LEADER_CONFIG, "");
      var alterConfigOp0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.DELETE);
      var configEntry1 = new ConfigEntry(ReplicationThrottlerImpl.THROTTLE_FOLLOWER_CONFIG, "");
      var alterConfigOp1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.DELETE);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

      Utils.packException(
          () ->
              admin.incrementalAlterConfigs(
                  Map.of(configResource, List.of(alterConfigOp0, alterConfigOp1))));
    }

    @Override
    public void clearReplicationThrottle(TopicPartition topicPartition) {
      clearReplicationThrottle(
          topicPartition.topic(), (replica) -> replica.partition() != topicPartition.partition());
    }

    @Override
    public void clearReplicationThrottle(org.astraea.app.admin.TopicPartitionReplica log) {
      clearReplicationThrottle(log.topic(), (replica) -> !replica.equals(log));
    }

    private void clearReplicationThrottle(
        String topic, Predicate<org.astraea.app.admin.TopicPartitionReplica> removePredicate) {
      var oldConfig = ReplicationThrottlerImpl.topicConfig(admin, Set.of(topic));
      var leaderConfigValue =
          oldConfig.get(topic).get(ReplicationThrottlerImpl.THROTTLE_LEADER_CONFIG).value();
      var followerConfigValue =
          oldConfig.get(topic).get(ReplicationThrottlerImpl.THROTTLE_FOLLOWER_CONFIG).value();
      var throttledLeaderReplicas =
          ReplicationThrottlerImpl.parseThrottleTarget(topic, leaderConfigValue);
      var throttledFollowerReplicas =
          ReplicationThrottlerImpl.parseThrottleTarget(topic, followerConfigValue);

      if (throttledLeaderReplicas == ReplicationThrottlerImpl.ALL_THROTTLED
          || throttledFollowerReplicas == ReplicationThrottlerImpl.ALL_THROTTLED)
        throw new UnsupportedOperationException(
            "No support for remove single throttle from the topic-wide throttle");

      var newLeaderThrottle =
          throttledLeaderReplicas.stream()
              .filter(removePredicate)
              .collect(Collectors.toUnmodifiableSet());
      var newFollowerThrottle =
          throttledFollowerReplicas.stream()
              .filter(removePredicate)
              .collect(Collectors.toUnmodifiableSet());

      var configEntry0 =
          new ConfigEntry(
              ReplicationThrottlerImpl.THROTTLE_LEADER_CONFIG,
              ReplicationThrottlerImpl.toThrottleTargetString(newLeaderThrottle));
      var alterConfigOp0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.SET);
      var configEntry1 =
          new ConfigEntry(
              ReplicationThrottlerImpl.THROTTLE_FOLLOWER_CONFIG,
              ReplicationThrottlerImpl.toThrottleTargetString(newFollowerThrottle));
      var alterConfigOp1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.SET);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      Utils.packException(
          () ->
              admin.incrementalAlterConfigs(
                  Map.of(configResource, List.of(alterConfigOp0, alterConfigOp1))));
    }
  }

  private static class ConfigImpl implements Config {
    private final Map<String, String> configs;

    ConfigImpl(org.apache.kafka.clients.admin.Config config) {
      this(
          config.entries().stream()
              .filter(e -> e.value() != null)
              .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));
    }

    ConfigImpl(Map<String, String> configs) {
      this.configs = Collections.unmodifiableMap(configs);
    }

    @Override
    public Optional<String> value(String key) {
      return Optional.ofNullable(configs.get(key));
    }

    @Override
    public Set<String> keys() {
      return configs.keySet();
    }

    @Override
    public Collection<String> values() {
      return configs.values();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return configs.entrySet().iterator();
    }
  }

  private static class CreatorImpl implements TopicCreator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<String, Map<TopicPartition, List<Replica>>> replicasGetter;
    private final Function<String, Config> configsGetter;
    private String topic;
    private int numberOfPartitions = 1;
    private short numberOfReplicas = 1;
    private final Map<String, String> configs = new HashMap<>();

    CreatorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<String, Map<TopicPartition, List<Replica>>> replicasGetter,
        Function<String, Config> configsGetter) {
      this.admin = admin;
      this.replicasGetter = replicasGetter;
      this.configsGetter = configsGetter;
    }

    @Override
    public TopicCreator topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
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
    public TopicCreator config(String key, String value) {
      this.configs.put(key, value);
      return this;
    }

    @Override
    public TopicCreator configs(Map<String, String> configs) {
      this.configs.putAll(configs);
      return this;
    }

    @Override
    public void create() {
      if (Utils.packException(() -> admin.listTopics().names().get()).contains(topic)) {
        var partitionReplicas = replicasGetter.apply(topic);
        partitionReplicas.forEach(
            (tp, replicas) -> {
              if (replicas.size() != numberOfReplicas)
                throw new IllegalArgumentException(
                    topic
                        + " is existent but its replicas: "
                        + replicas.size()
                        + " is not equal to expected: "
                        + numberOfReplicas);
            });
        var result =
            Utils.packException(() -> admin.describeTopics(Set.of(topic)).all().get().get(topic));
        if (result.partitions().size() != numberOfPartitions)
          throw new IllegalArgumentException(
              topic
                  + " is existent but its partitions: "
                  + result.partitions().size()
                  + " is not equal to expected: "
                  + numberOfReplicas);

        var actualConfigs = configsGetter.apply(topic);
        this.configs.forEach(
            (key, value) -> {
              if (actualConfigs.value(key).filter(actual -> actual.equals(value)).isEmpty())
                throw new IllegalArgumentException(
                    topic
                        + " is existent but its config: <"
                        + key
                        + ", "
                        + actualConfigs.value(key)
                        + "> is not equal to expected: "
                        + key
                        + ", "
                        + value);
            });

        // ok, the existent topic is totally equal to what we want to create.
        return;
      }

      Utils.packException(
          () ->
              admin
                  .createTopics(
                      List.of(
                          new NewTopic(topic, numberOfPartitions, numberOfReplicas)
                              .configs(configs)))
                  .all()
                  .get());
    }
  }

  private static class MigratorImpl implements ReplicaMigrator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<Set<String>, Set<TopicPartition>> partitionGetter;
    private final Set<TopicPartition> partitions = new HashSet<>();

    MigratorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<Set<String>, Set<TopicPartition>> partitionGetter) {
      this.admin = admin;
      this.partitionGetter = partitionGetter;
    }

    @Override
    public ReplicaMigrator topic(String topic) {
      partitions.addAll(partitionGetter.apply(Set.of(topic)));
      return this;
    }

    @Override
    public ReplicaMigrator partition(String topic, int partition) {
      partitions.add(TopicPartition.of(topic, partition));
      return this;
    }

    @Override
    public void moveTo(Map<Integer, String> brokerFolders) {
      // ensure this partition is host on the given map
      var topicPartition = partitions.iterator().next();
      var currentReplicas =
          Utils.packException(
                  () -> admin.describeTopics(Set.of(topicPartition.topic())).allTopicNames().get())
              .get(topicPartition.topic())
              .partitions()
              .get(topicPartition.partition())
              .replicas()
              .stream()
              .map(Node::id)
              .collect(Collectors.toUnmodifiableSet());
      var notHere =
          brokerFolders.keySet().stream()
              .filter(id -> !currentReplicas.contains(id))
              .collect(Collectors.toUnmodifiableSet());

      if (!notHere.isEmpty())
        throw new IllegalStateException(
            "The following specified broker is not part of the replica list: " + notHere);

      var payload =
          brokerFolders.entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      entry ->
                          new TopicPartitionReplica(
                              topicPartition.topic(), topicPartition.partition(), entry.getKey()),
                      Map.Entry::getValue));
      Utils.packException(() -> admin.alterReplicaLogDirs(payload).all().get());
    }

    @Override
    public void declarePreferredDir(Map<Integer, String> preferredDirMap) {
      // ensure this partition is not host on the given map
      var topicPartition = partitions.iterator().next();
      var currentReplicas =
          Utils.packException(
                  () -> admin.describeTopics(Set.of(topicPartition.topic())).allTopicNames().get())
              .get(topicPartition.topic())
              .partitions()
              .get(topicPartition.partition())
              .replicas();
      var alreadyHere =
          currentReplicas.stream()
              .map(Node::id)
              .filter(preferredDirMap::containsKey)
              .collect(Collectors.toUnmodifiableSet());

      if (!alreadyHere.isEmpty())
        throw new IllegalStateException(
            "The following specified broker is already part of the replica list: " + alreadyHere);

      try {
        var payload =
            preferredDirMap.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        entry ->
                            new TopicPartitionReplica(
                                topicPartition.topic(), topicPartition.partition(), entry.getKey()),
                        Map.Entry::getValue));
        Utils.packException(() -> admin.alterReplicaLogDirs(payload).all().get());
      } catch (ReplicaNotAvailableException ignore) {
        // The call is probably trying to declare the preferred data directory. Swallow the
        // exception since this is a supported operation. See the Javadoc of
        // AdminClient#alterReplicaLogDirs for details.
      }
    }

    @Override
    public void moveTo(List<Integer> brokers) {
      Utils.packException(
          () ->
              admin
                  .alterPartitionReassignments(
                      partitions.stream()
                          .collect(
                              Collectors.toMap(
                                  TopicPartition::to,
                                  ignore -> Optional.of(new NewPartitionReassignment(brokers)))))
                  .all()
                  .get());
    }
  }

  private static class QuotaImpl implements QuotaCreator {
    private final org.apache.kafka.clients.admin.Admin admin;

    QuotaImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = admin;
    }

    @Override
    public Ip ip(String ip) {
      return new Ip() {
        private int connectionRate = Integer.MAX_VALUE;

        @Override
        public Ip connectionRate(int value) {
          this.connectionRate = value;
          return this;
        }

        @Override
        public void create() {
          if (connectionRate == Integer.MAX_VALUE) return;
          Utils.packException(
              () ->
                  admin
                      .alterClientQuotas(
                          List.of(
                              new ClientQuotaAlteration(
                                  new ClientQuotaEntity(Map.of(ClientQuotaEntity.IP, ip)),
                                  List.of(
                                      new ClientQuotaAlteration.Op(
                                          Quota.Limit.IP_CONNECTION_RATE.nameOfKafka(),
                                          (double) connectionRate)))))
                      .all()
                      .get());
        }
      };
    }

    @Override
    public Client clientId(String id) {
      return new Client() {
        private DataRate produceRate = null;
        private DataRate consumeRate = null;

        @Override
        public Client produceRate(DataRate value) {
          this.produceRate = value;
          return this;
        }

        @Override
        public Client consumeRate(DataRate value) {
          this.consumeRate = value;
          return this;
        }

        @Override
        public void create() {
          var q = new ArrayList<ClientQuotaAlteration.Op>();
          if (produceRate != null)
            q.add(
                new ClientQuotaAlteration.Op(
                    Quota.Limit.PRODUCER_BYTE_RATE.nameOfKafka(), produceRate.byteRate()));
          if (consumeRate != null)
            q.add(
                new ClientQuotaAlteration.Op(
                    Quota.Limit.CONSUMER_BYTE_RATE.nameOfKafka(), consumeRate.byteRate()));
          if (!q.isEmpty())
            Utils.packException(
                () ->
                    admin
                        .alterClientQuotas(
                            List.of(
                                new ClientQuotaAlteration(
                                    new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, id)),
                                    q)))
                        .all()
                        .get());
        }
      };
    }
  }

  private static class ReplicationThrottlerImpl implements ReplicationThrottler {

    private static class ThrottleTarget {
      final Set<String> wholeTopic = new HashSet<>();
      final Map<String, Set<org.astraea.app.admin.TopicPartitionReplica>> leader = new HashMap<>();
      final Map<String, Set<org.astraea.app.admin.TopicPartitionReplica>> follower =
          new HashMap<>();
    }

    private final org.apache.kafka.clients.admin.Admin admin;
    private DataRate defaultIngress = null;
    private DataRate defaultEgress = null;
    private final Map<Integer, DataRate> ingressMap = new HashMap<>();
    private final Map<Integer, DataRate> egressMap = new HashMap<>();
    private final List<Consumer<ThrottleTarget>> throttleCalculation = new ArrayList<>();

    public ReplicationThrottlerImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = admin;
    }

    @Override
    public ReplicationThrottler ingress(DataRate limitForEachFollowerBroker) {
      this.defaultIngress = limitForEachFollowerBroker;
      return this;
    }

    @Override
    public ReplicationThrottler ingress(Map<Integer, DataRate> limitPerFollowerBroker) {
      this.ingressMap.putAll(limitPerFollowerBroker);
      return this;
    }

    @Override
    public ReplicationThrottler egress(DataRate limitForEachLeaderBroker) {
      this.defaultEgress = limitForEachLeaderBroker;
      return this;
    }

    @Override
    public ReplicationThrottler egress(Map<Integer, DataRate> limitPerLeaderBroker) {
      this.egressMap.putAll(limitPerLeaderBroker);
      return this;
    }

    @Override
    public ReplicationThrottler throttle(String topic) {
      throttleCalculation.add(throttleTarget -> throttleTarget.wholeTopic.add(topic));
      return this;
    }

    @Override
    public ReplicationThrottler throttle(TopicPartition topicPartition) {
      throttleCalculation.add(
          throttleTarget -> {
            final var topic = topicPartition.topic();
            final var partition = topicPartition.partition();
            final var replicas =
                Utils.packException(
                    () ->
                        admin
                            .describeTopics(Set.of(topic))
                            .allTopicNames()
                            .get()
                            .get(topic)
                            .partitions()
                            .get(partition)
                            .replicas()
                            .stream()
                            .map(Node::id)
                            .map(
                                id ->
                                    org.astraea.app.admin.TopicPartitionReplica.of(
                                        topic, partition, id))
                            .collect(Collectors.toUnmodifiableList()));

            throttleTarget
                .leader
                .computeIfAbsent(topic, (ignore) -> new HashSet<>())
                .add(replicas.stream().limit(1).findFirst().orElseThrow());
            throttleTarget
                .follower
                .computeIfAbsent(topic, (ignore) -> new HashSet<>())
                .addAll(replicas.stream().skip(1).collect(Collectors.toUnmodifiableList()));
          });
      return this;
    }

    @Override
    public ReplicationThrottler throttle(org.astraea.app.admin.TopicPartitionReplica replica) {
      throttleCalculation.add(
          throttleTarget -> {
            final var topic = replica.topic();
            final var partition = replica.partition();
            final var brokerId = replica.brokerId();
            final var replicas =
                Utils.packException(
                    () ->
                        admin
                            .describeTopics(Set.of(topic))
                            .allTopicNames()
                            .get()
                            .get(topic)
                            .partitions()
                            .get(partition)
                            .replicas()
                            .stream()
                            .map(Node::id)
                            .map(
                                id ->
                                    org.astraea.app.admin.TopicPartitionReplica.of(
                                        topic, partition, id))
                            .collect(Collectors.toUnmodifiableList()));

            final var indexOf =
                IntStream.range(0, replicas.size())
                    .filter(i -> replicas.get(i).brokerId() == brokerId)
                    .findFirst()
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                "This replica "
                                    + replica
                                    + " is not part of the topic \""
                                    + replica.topic()
                                    + "\""));

            (indexOf == 0 ? throttleTarget.leader : throttleTarget.follower)
                .computeIfAbsent(topic, (ignore) -> new HashSet<>())
                .add(replica);
          });
      return this;
    }

    @Override
    public ReplicationThrottler throttleLeader(
        org.astraea.app.admin.TopicPartitionReplica replica) {
      throttleCalculation.add(
          throttleTarget ->
              throttleTarget
                  .leader
                  .computeIfAbsent(replica.topic(), (ignore) -> new HashSet<>())
                  .add(replica));
      return this;
    }

    @Override
    public ReplicationThrottler throttleFollower(
        org.astraea.app.admin.TopicPartitionReplica replica) {
      throttleCalculation.add(
          throttleTarget ->
              throttleTarget
                  .follower
                  .computeIfAbsent(replica.topic(), (ignore) -> new HashSet<>())
                  .add(replica));
      return this;
    }

    @Override
    public void apply() {
      // attempt to get the throttle targets
      var throttleTarget = new ThrottleTarget();
      throttleCalculation.forEach(x -> x.accept(throttleTarget));

      // apply
      applyBandwidthThrottle(throttleTarget);
      applyLogThrottle(throttleTarget);
    }

    private void applyBandwidthThrottle(ThrottleTarget throttleTarget) {
      Set<Integer> affectedLeaderBrokers =
          throttleTarget.leader.values().stream()
              .flatMap(Collection::stream)
              .map(org.astraea.app.admin.TopicPartitionReplica::brokerId)
              .collect(Collectors.toUnmodifiableSet());
      Set<Integer> affectedFollowerBrokers =
          throttleTarget.follower.values().stream()
              .flatMap(Collection::stream)
              .map(org.astraea.app.admin.TopicPartitionReplica::brokerId)
              .collect(Collectors.toUnmodifiableSet());
      Set<Integer> affectedBrokersViaTopic =
          Utils.packException(
                  () -> admin.describeTopics(throttleTarget.wholeTopic).allTopicNames().get())
              .entrySet()
              .stream()
              .flatMap(x -> x.getValue().partitions().stream())
              .flatMap(x -> x.replicas().stream())
              .map(Node::id)
              .collect(Collectors.toUnmodifiableSet());

      Function<Integer, DataRate> getDefaultEgress =
          (broker) -> {
            if (defaultEgress == null)
              throw new IllegalArgumentException(
                  "No egress bandwidth specified for broker " + broker);
            return defaultEgress;
          };
      Function<Integer, DataRate> getDefaultIngress =
          (broker) -> {
            if (defaultIngress == null)
              throw new IllegalArgumentException(
                  "No ingress bandwidth specified for broker " + broker);
            return defaultIngress;
          };

      var updatedEgressMap = new HashMap<>(egressMap);
      for (int broker : affectedLeaderBrokers)
        updatedEgressMap.putIfAbsent(broker, getDefaultEgress.apply(broker));
      for (int broker : affectedBrokersViaTopic)
        updatedEgressMap.putIfAbsent(broker, getDefaultEgress.apply(broker));

      var updatedIngressMap = new HashMap<>(ingressMap);
      for (int broker : affectedFollowerBrokers)
        updatedIngressMap.putIfAbsent(broker, getDefaultIngress.apply(broker));
      for (int broker : affectedBrokersViaTopic)
        updatedIngressMap.putIfAbsent(broker, getDefaultIngress.apply(broker));

      throttleBandwidth(updatedEgressMap, updatedIngressMap);
    }

    private void applyLogThrottle(ThrottleTarget throttleTarget) {
      // Since the throttle work in a declarative manner, we have to fetch existing config to do the
      // old/new config merge. Whole topic target is ignored because it is going to be complete
      // throttled later. So no need to do this fancy stuff.
      var affectedTopics =
          Stream.of(
                  throttleTarget.leader.keySet().stream(),
                  throttleTarget.follower.keySet().stream())
              .flatMap(x -> x)
              .collect(Collectors.toUnmodifiableSet());
      var originalConfigs = topicConfig(admin, affectedTopics);
      var mergedConfig =
          (Function<
                  String,
                  Function<
                      Map<String, Set<org.astraea.app.admin.TopicPartitionReplica>>,
                      Map<String, Set<org.astraea.app.admin.TopicPartitionReplica>>>>)
              (config) -> {
                // this function is doing the old/new config merge
                var oldConfig =
                    originalConfigs.entrySet().stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                entry ->
                                    parseThrottleTarget(
                                        entry.getKey(), entry.getValue().get(config).value())));

                return (newConfigs) ->
                    newConfigs.entrySet().stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                x ->
                                    Stream.concat(
                                            x.getValue().stream(),
                                            oldConfig.getOrDefault(x.getKey(), Set.of()).stream())
                                        .collect(Collectors.toUnmodifiableSet())));
              };
      var toConfigValue =
          (Function<
                  Map.Entry<String, Set<org.astraea.app.admin.TopicPartitionReplica>>,
                  Map.Entry<String, String>>)
              (entry) -> {
                // this config convert replicas into string config value format
                var topicName = entry.getKey();
                var replicas = entry.getValue();
                var configValue = toThrottleTargetString(replicas);
                return Map.entry(topicName, configValue);
              };
      var allThrottleMap =
          (Supplier<Map<String, Set<org.astraea.app.admin.TopicPartitionReplica>>>)
              () ->
                  throttleTarget.wholeTopic.stream()
                      .collect(Collectors.toUnmodifiableMap(x -> x, x -> ALL_THROTTLED));
      var updateLeaderConfig =
          Stream.concat(
                  mergedConfig
                      .apply(THROTTLE_LEADER_CONFIG)
                      .apply(throttleTarget.leader)
                      .entrySet()
                      .stream(),
                  allThrottleMap.get().entrySet().stream())
              .map(toConfigValue)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      var updateFollowerConfig =
          Stream.concat(
                  mergedConfig
                      .apply(THROTTLE_FOLLOWER_CONFIG)
                      .apply(throttleTarget.follower)
                      .entrySet()
                      .stream(),
                  allThrottleMap.get().entrySet().stream())
              .map(toConfigValue)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      throttleLog(updateLeaderConfig, updateFollowerConfig);
    }

    private static final String THROTTLE_LEADER_BANDWIDTH_CONFIG =
        "leader.replication.throttled.rate";
    private static final String THROTTLE_FOLLOWER_BANDWIDTH_CONFIG =
        "follower.replication.throttled.rate";

    private void throttleBandwidth(
        Map<Integer, DataRate> leaderSetting, Map<Integer, DataRate> followerSetting) {
      var toConfigEntry =
          (Function<
                  String,
                  Function<Map.Entry<Integer, DataRate>, Map.Entry<ConfigResource, AlterConfigOp>>>)
              (configName) ->
                  (entry) -> {
                    var broker = entry.getKey();
                    var byteRate = (long) entry.getValue().byteRate();
                    var configEntry = new ConfigEntry(configName, Long.toString(byteRate));
                    var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
                    var configResource =
                        new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(broker));
                    return Map.entry(configResource, alterConfigOp);
                  };
      var egressSettings =
          leaderSetting.entrySet().stream()
              .map(toConfigEntry.apply(THROTTLE_LEADER_BANDWIDTH_CONFIG));
      var ingressSettings =
          followerSetting.entrySet().stream()
              .map(toConfigEntry.apply(THROTTLE_FOLLOWER_BANDWIDTH_CONFIG));
      var combinedConfigs =
          Stream.concat(egressSettings, ingressSettings)
              .collect(
                  Collectors.groupingBy(
                      Map.Entry::getKey,
                      Collectors.mapping(
                          Map.Entry::getValue,
                          Collectors.toCollection(
                              () -> (Collection<AlterConfigOp>) new ArrayList<AlterConfigOp>()))));
      Utils.packException(() -> admin.incrementalAlterConfigs(combinedConfigs));
    }

    private static final String THROTTLE_LEADER_CONFIG = "leader.replication.throttled.replicas";
    private static final String THROTTLE_FOLLOWER_CONFIG =
        "follower.replication.throttled.replicas";
    private static final Set<org.astraea.app.admin.TopicPartitionReplica> ALL_THROTTLED =
        Stream.<org.astraea.app.admin.TopicPartitionReplica>of()
            .collect(Collectors.toUnmodifiableSet());

    private void throttleLog(
        Map<String, String> leaderSetting, Map<String, String> followerSetting) {
      var toConfigEntry =
          (Function<
                  String,
                  Function<Map.Entry<String, String>, Map.Entry<ConfigResource, AlterConfigOp>>>)
              (configName) ->
                  (entry) -> {
                    var topic = entry.getKey();
                    var config = entry.getValue();
                    var configEntry = new ConfigEntry(configName, config);
                    var alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
                    var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                    return Map.entry(configResource, alterConfigOp);
                  };
      var leaderConfigs =
          leaderSetting.entrySet().stream().map(toConfigEntry.apply(THROTTLE_LEADER_CONFIG));
      var followerConfigs =
          followerSetting.entrySet().stream().map(toConfigEntry.apply(THROTTLE_FOLLOWER_CONFIG));
      var combinedConfigs =
          Stream.concat(leaderConfigs, followerConfigs)
              .collect(
                  Collectors.groupingBy(
                      Map.Entry::getKey,
                      Collectors.mapping(
                          Map.Entry::getValue,
                          Collectors.toCollection(
                              () -> (Collection<AlterConfigOp>) new ArrayList<AlterConfigOp>()))));
      Utils.packException(() -> admin.incrementalAlterConfigs(combinedConfigs).all().get());
    }

    /**
     * @return the set of replica that the {@link ReplicationThrottlerImpl#THROTTLE_LEADER_CONFIG}
     *     or {@link ReplicationThrottlerImpl#THROTTLE_FOLLOWER_CONFIG} config value declared to
     *     throttle. If the value is a wildcard, a special set {@link
     *     ReplicationThrottlerImpl#ALL_THROTTLED} will be returned. Which mean the whole topic is
     *     throttled.
     */
    private static Set<org.astraea.app.admin.TopicPartitionReplica> parseThrottleTarget(
        String topic, String configValue) {
      // if the config value is wildcard, just ignore it since everything is throttled & this
      // interface doesn't support removal.
      if (configValue.equals("*")) return ALL_THROTTLED;
      if (configValue.equals("")) return Set.of();
      // convert the throttle pair into a replica set.
      return Arrays.stream(configValue.split(","))
          .map(item -> item.split(":"))
          .map(str -> List.of(Integer.valueOf(str[0]), Integer.valueOf(str[1])))
          .map(
              ints ->
                  org.astraea.app.admin.TopicPartitionReplica.of(topic, ints.get(0), ints.get(1)))
          .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * @return the suitable string for the given log throttle setting. If the given set is the
     *     {@link ReplicationThrottlerImpl#ALL_THROTTLED} instance, then a {@code "*"} string will
     *     be return.
     */
    private static String toThrottleTargetString(
        Set<org.astraea.app.admin.TopicPartitionReplica> replicas) {
      if (replicas == ALL_THROTTLED) return "*";
      return replicas.stream()
          .map(replica -> replica.partition() + ":" + replica.brokerId())
          .collect(Collectors.joining(","));
    }

    private static Map<String, org.apache.kafka.clients.admin.Config> topicConfig(
        org.apache.kafka.clients.admin.Admin admin, Set<String> topics) {
      var configResources =
          topics.stream()
              .map(s -> new ConfigResource(ConfigResource.Type.TOPIC, s))
              .collect(Collectors.toUnmodifiableList());
      return Utils.packException(() -> admin.describeConfigs(configResources).all().get())
          .entrySet()
          .stream()
          .collect(Collectors.toUnmodifiableMap(x -> x.getKey().name(), Map.Entry::getValue));
    }
  }
}
