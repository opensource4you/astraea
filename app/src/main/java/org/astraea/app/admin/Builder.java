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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.ExecutionRuntimeException;
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
      } catch (ExecutionRuntimeException executionRuntimeException) {
        if (ElectionNotNeededException.class
            != executionRuntimeException.getRootCause().getClass()) {
          throw executionRuntimeException;
        }
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
    public Set<String> topicNames(boolean listInternal) {
      return Utils.packException(
          () -> admin.listTopics(new ListTopicsOptions().listInternal(listInternal)).names().get());
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
              e -> e.getValue().stream().map(replica -> Map.entry(replica.nodeInfo(), e.getKey())))
          .filter(e -> brokerIds.contains(e.getKey().id()))
          .collect(Collectors.groupingBy(e -> e.getKey().id()))
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> e.getValue().stream().map(Map.Entry::getValue).collect(Collectors.toSet())));
    }

    @Override
    public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
      var logInfo =
          Utils.packException(() -> admin.describeLogDirs(brokerIds()).allDescriptions().get());

      BiFunction<
              org.apache.kafka.common.TopicPartition,
              org.apache.kafka.common.TopicPartitionInfo,
              List<Replica>>
          toReplicas =
              (tp, tpi) ->
                  tpi.replicas().stream()
                      .map(
                          node -> {
                            var dataPath =
                                node.isEmpty()
                                    ? null
                                    : logInfo.get(node.id()).entrySet().stream()
                                        .filter(e -> e.getValue().replicaInfos().containsKey(tp))
                                        .map(Map.Entry::getKey)
                                        .findFirst()
                                        .orElse(null);
                            var replicaInfo =
                                node.isEmpty() || dataPath == null
                                    ? null
                                    : logInfo.get(node.id()).get(dataPath).replicaInfos().get(tp);
                            return Replica.of(
                                tp.topic(),
                                tp.partition(),
                                NodeInfo.of(node),
                                replicaInfo != null ? replicaInfo.offsetLag() : -1L,
                                replicaInfo != null ? replicaInfo.size() : -1L,
                                !tpi.leader().isEmpty() && tpi.leader().id() == node.id(),
                                tpi.isr().contains(node),
                                replicaInfo != null && replicaInfo.isFuture(),
                                node.isEmpty(),
                                // The first replica in the return result is the
                                // preferred leader. This only works with Kafka broker version
                                // after 0.11.
                                // Version before 0.11 returns the replicas in unspecified order
                                // due to a bug.
                                tpi.replicas().get(0).id() == node.id(),
                                dataPath);
                          })
                      .collect(Collectors.toList());

      return Utils.packException(
              () ->
                  admin.describeTopics(topics).allTopicNames().get().entrySet().stream()
                      .flatMap(
                          e ->
                              e.getValue().partitions().stream()
                                  .map(
                                      tpInfo ->
                                          Map.entry(
                                              new org.apache.kafka.common.TopicPartition(
                                                  e.getKey(), tpInfo.partition()),
                                              tpInfo))))
          .collect(
              Collectors.toUnmodifiableMap(
                  e -> TopicPartition.from(e.getKey()),
                  entry -> toReplicas.apply(entry.getKey(), entry.getValue())));
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

      var replicas =
          Utils.packException(
              () ->
                  replicas(topics).values().stream()
                      .flatMap(Collection::stream)
                      .map(r -> (ReplicaInfo) r)
                      .collect(Collectors.toUnmodifiableList()));

      return new ClusterInfo() {
        @Override
        public List<NodeInfo> nodes() {
          return nodeInfo;
        }

        @Override
        public List<ReplicaInfo> replicas() {
          return replicas;
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
      } catch (ExecutionRuntimeException executionRuntimeException) {
        var rootCause = executionRuntimeException.getRootCause();
        if (IllegalArgumentException.class == rootCause.getClass()
            && ERROR_MSG_MEMBER_IS_EMPTY.equals(rootCause.getMessage())) {
          // Deleting all members can't work when there is no members already.
          return;
        }
        throw executionRuntimeException;
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
                                                      r.nodeInfo().id())))
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
      return new ReplicationThrottler() {

        private final Map<Integer, DataRate> egress = new HashMap<>();
        private final Map<Integer, DataRate> ingress = new HashMap<>();
        private final Set<TopicPartitionReplica> leaders = new HashSet<>();
        private final Set<TopicPartitionReplica> followers = new HashSet<>();

        @Override
        public ReplicationThrottler ingress(DataRate limitForEachFollowerBroker) {
          brokerIds().forEach(id -> ingress.put(id, limitForEachFollowerBroker));
          return this;
        }

        @Override
        public ReplicationThrottler ingress(Map<Integer, DataRate> limitPerFollowerBroker) {
          ingress.putAll(limitPerFollowerBroker);
          return this;
        }

        @Override
        public ReplicationThrottler egress(DataRate limitForEachLeaderBroker) {
          brokerIds().forEach(id -> egress.put(id, limitForEachLeaderBroker));
          return this;
        }

        @Override
        public ReplicationThrottler egress(Map<Integer, DataRate> limitPerLeaderBroker) {
          egress.putAll(limitPerLeaderBroker);
          return this;
        }

        @Override
        public ReplicationThrottler throttle(String topic) {
          replicas(Set.of(topic))
              .forEach(
                  (tp, replicas) -> {
                    replicas.forEach(
                        replica -> {
                          if (replica.isLeader())
                            leaders.add(
                                TopicPartitionReplica.of(
                                    tp.topic(), tp.partition(), replica.nodeInfo().id()));
                          else
                            followers.add(
                                TopicPartitionReplica.of(
                                    tp.topic(), tp.partition(), replica.nodeInfo().id()));
                        });
                  });
          return this;
        }

        @Override
        public ReplicationThrottler throttle(TopicPartition topicPartition) {
          var replicas =
              replicas(Set.of(topicPartition.topic())).getOrDefault(topicPartition, List.of());
          replicas.forEach(
              replica -> {
                if (replica.isLeader())
                  leaders.add(
                      TopicPartitionReplica.of(
                          topicPartition.topic(),
                          topicPartition.partition(),
                          replica.nodeInfo().id()));
                else
                  followers.add(
                      TopicPartitionReplica.of(
                          topicPartition.topic(),
                          topicPartition.partition(),
                          replica.nodeInfo().id()));
              });
          return this;
        }

        @Override
        public ReplicationThrottler throttle(TopicPartitionReplica replica) {
          leaders.add(replica);
          followers.add(replica);
          return this;
        }

        @Override
        public ReplicationThrottler throttleLeader(TopicPartitionReplica replica) {
          leaders.add(replica);
          return this;
        }

        @Override
        public ReplicationThrottler throttleFollower(TopicPartitionReplica replica) {
          followers.add(replica);
          return this;
        }

        @Override
        public void apply() {
          applyBandwidth();
          applyThrottledReplicas();
        }

        private void applyThrottledReplicas() {
          // Attempt to fetch the current value of log throttle config. If the config value is
          // empty, we have to perform an `AlterConfigOp.OpType.SET` operation instead of an
          // `AlterConfigOp.OpType.APPEND` operation for the log throttle config. We have to do this
          // to work around the https://github.com/apache/kafka/pull/12503 bug.
          // TODO: remove this workaround in appropriate time. see #584
          var configValues =
              Utils.packException(
                  () ->
                      admin
                          .describeConfigs(
                              Stream.concat(leaders.stream(), followers.stream())
                                  .map(TopicPartitionReplica::topic)
                                  .map(
                                      topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                                  .collect(Collectors.toSet()))
                          .all()
                          .get());
          BiFunction<
                  Set<TopicPartitionReplica>,
                  String,
                  Map<ConfigResource, Collection<AlterConfigOp>>>
              toKafkaConfigs =
                  (replicas, key) ->
                      replicas.stream()
                          .collect(Collectors.groupingBy(TopicPartitionReplica::topic))
                          .entrySet()
                          .stream()
                          .collect(
                              Collectors.toMap(
                                  e ->
                                      new ConfigResource(
                                          ConfigResource.Type.TOPIC, String.valueOf(e.getKey())),
                                  e -> {
                                    var oldValue =
                                        configValues
                                            .get(
                                                new ConfigResource(
                                                    ConfigResource.Type.TOPIC,
                                                    String.valueOf(e.getKey())))
                                            .get(key)
                                            .value();

                                    // partition/broker based throttle setting can't be used in
                                    // conjunction with wildcard throttle. This is a limitation in
                                    // the kafka implementation.
                                    if (oldValue.equals("*"))
                                      throw new UnsupportedOperationException(
                                          "This API doesn't support wildcard throttle");

                                    var configValue =
                                        e.getValue().stream()
                                            .map(
                                                replica ->
                                                    replica.partition() + ":" + replica.brokerId())
                                            .collect(Collectors.joining(","));
                                    // work around a bug https://github.com/apache/kafka/pull/12503
                                    var operation =
                                        oldValue.isEmpty()
                                            ? AlterConfigOp.OpType.SET
                                            : AlterConfigOp.OpType.APPEND;
                                    var entry = new ConfigEntry(key, configValue);
                                    var alter = new AlterConfigOp(entry, operation);

                                    return List.of(alter);
                                  }));
          if (!leaders.isEmpty())
            Utils.packException(
                () ->
                    admin
                        .incrementalAlterConfigs(
                            toKafkaConfigs.apply(leaders, "leader.replication.throttled.replicas"))
                        .all()
                        .get());

          if (!followers.isEmpty())
            Utils.packException(
                () ->
                    admin
                        .incrementalAlterConfigs(
                            toKafkaConfigs.apply(
                                followers, "follower.replication.throttled.replicas"))
                        .all()
                        .get());
        }

        private void applyBandwidth() {
          BiFunction<Map<Integer, DataRate>, String, Map<ConfigResource, Collection<AlterConfigOp>>>
              toKafkaConfigs =
                  (raw, key) ->
                      raw.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  e ->
                                      new ConfigResource(
                                          ConfigResource.Type.BROKER, String.valueOf(e.getKey())),
                                  e ->
                                      List.of(
                                          new AlterConfigOp(
                                              new ConfigEntry(
                                                  key,
                                                  String.valueOf((long) e.getValue().byteRate())),
                                              AlterConfigOp.OpType.SET))));

          if (!egress.isEmpty())
            Utils.packException(
                () ->
                    admin
                        .incrementalAlterConfigs(
                            toKafkaConfigs.apply(egress, "leader.replication.throttled.rate"))
                        .all()
                        .get());

          if (!ingress.isEmpty())
            Utils.packException(
                () ->
                    admin
                        .incrementalAlterConfigs(
                            toKafkaConfigs.apply(ingress, "follower.replication.throttled.rate"))
                        .all()
                        .get());
        }
      };
    }

    @Override
    public void clearReplicationThrottle(String topic) {
      var configEntry0 = new ConfigEntry("leader.replication.throttled.replicas", "");
      var alterConfigOp0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.DELETE);
      var configEntry1 = new ConfigEntry("follower.replication.throttled.replicas", "");
      var alterConfigOp1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.DELETE);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

      Utils.packException(
          () ->
              admin.incrementalAlterConfigs(
                  Map.of(configResource, List.of(alterConfigOp0, alterConfigOp1))));
    }

    @Override
    public void clearReplicationThrottle(TopicPartition topicPartition) {
      var configValue =
          replicas(Set.of(topicPartition.topic())).get(topicPartition).stream()
              .map(replica -> topicPartition.partition() + ":" + replica.nodeInfo().id())
              .collect(Collectors.joining(","));
      var configEntry0 = new ConfigEntry("leader.replication.throttled.replicas", configValue);
      var configEntry1 = new ConfigEntry("follower.replication.throttled.replicas", configValue);
      var alterConfigOp0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.SUBTRACT);
      var alterConfigOp1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.SUBTRACT);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicPartition.topic());
      Utils.packException(
          () ->
              admin.incrementalAlterConfigs(
                  Map.of(configResource, List.of(alterConfigOp0, alterConfigOp1))));
    }

    @Override
    public void clearReplicationThrottle(TopicPartitionReplica log) {
      var configValue = log.partition() + ":" + log.brokerId();
      var configEntry0 = new ConfigEntry("leader.replication.throttled.replicas", configValue);
      var configEntry1 = new ConfigEntry("follower.replication.throttled.replicas", configValue);
      var alterConfigOp0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.SUBTRACT);
      var alterConfigOp1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.SUBTRACT);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, log.topic());
      Utils.packException(
          () ->
              admin.incrementalAlterConfigs(
                  Map.of(configResource, List.of(alterConfigOp0, alterConfigOp1))));
    }

    @Override
    public void clearIngressReplicationThrottle(Set<Integer> brokerIds) {
      deleteBrokerConfigs(
          brokerIds.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      id -> id, id -> Set.of("follower.replication.throttled.rate"))));
    }

    @Override
    public void clearEgressReplicationThrottle(Set<Integer> brokerIds) {
      deleteBrokerConfigs(
          brokerIds.stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      id -> id, id -> Set.of("leader.replication.throttled.rate"))));
    }

    private void deleteBrokerConfigs(Map<Integer, Set<String>> brokerAndConfigKeys) {
      Function<String, AlterConfigOp> deleteConfig =
          (key) -> new AlterConfigOp(new ConfigEntry(key, ""), AlterConfigOp.OpType.DELETE);
      var map =
          brokerAndConfigKeys.entrySet().stream()
              .map(
                  entry ->
                      Map.entry(
                          String.valueOf(entry.getKey()),
                          entry.getValue().stream()
                              .map(deleteConfig)
                              .collect(Collectors.toUnmodifiableList())))
              .collect(
                  Collectors.toUnmodifiableMap(
                      entry -> new ConfigResource(ConfigResource.Type.BROKER, entry.getKey()),
                      entry -> (Collection<AlterConfigOp>) entry.getValue()));
      Utils.packException(() -> admin.incrementalAlterConfigs(map).all().get());
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
                          new org.apache.kafka.common.TopicPartitionReplica(
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
                            new org.apache.kafka.common.TopicPartitionReplica(
                                topicPartition.topic(), topicPartition.partition(), entry.getKey()),
                        Map.Entry::getValue));
        Utils.packException(() -> admin.alterReplicaLogDirs(payload).all().get());
      } catch (ExecutionRuntimeException executionRuntimeException) {
        if (ReplicaNotAvailableException.class
            != executionRuntimeException.getRootCause().getClass()) {
          throw executionRuntimeException;
        }
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
}
