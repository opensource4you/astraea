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
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.astraea.common.DataRate;
import org.astraea.common.ExecutionRuntimeException;
import org.astraea.common.Utils;

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

    private final AsyncAdmin asyncAdmin;

    private final String clientId;
    private final List<?> pendingRequests;

    AdminImpl(org.apache.kafka.clients.admin.Admin admin) {
      this.admin = Objects.requireNonNull(admin);
      this.asyncAdmin = AsyncAdmin.of(admin);
      this.clientId = (String) Utils.member(admin, "clientId");
      this.pendingRequests =
          (ArrayList<?>) Utils.member(Utils.member(admin, "runnable"), "pendingCalls");
    }

    @Override
    public void close() {
      admin.close();
    }

    @Override
    public ReplicaMigrator migrator() {
      return new MigratorImpl(admin, this::topicPartitions, this::topicPartitions);
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
    public List<ProducerState> producerStates(Set<TopicPartition> partitions) {
      return Utils.packException(
          () -> asyncAdmin.producerStates(partitions).toCompletableFuture().get());
    }

    @Override
    public Set<String> consumerGroupIds() {
      return Utils.packException(() -> asyncAdmin.consumerGroupIds().toCompletableFuture().get());
    }

    @Override
    public List<ConsumerGroup> consumerGroups(Set<String> consumerGroupNames) {
      return Utils.packException(
          () -> asyncAdmin.consumerGroups(consumerGroupNames).toCompletableFuture().get());
    }

    @Override
    public List<Topic> topics(Set<String> names) {
      return Utils.packException(() -> asyncAdmin.topics(names).toCompletableFuture().get());
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
    public Set<String> topicNames(boolean listInternal) {
      return Utils.packException(
          () -> asyncAdmin.topicNames(listInternal).toCompletableFuture().get());
    }

    @Override
    public void deleteTopics(Set<String> topics) {
      Utils.packException(() -> asyncAdmin.deleteTopics(topics).toCompletableFuture().get());
    }

    @Override
    public Set<NodeInfo> nodes() {
      return Utils.packException(() -> asyncAdmin.nodeInfos().toCompletableFuture().get());
    }

    @Override
    public List<Broker> brokers() {
      return Utils.packException(() -> asyncAdmin.brokers().toCompletableFuture().get());
    }

    @Override
    public List<Partition> partitions(Set<String> topics) {
      return Utils.packException(() -> asyncAdmin.partitions(topics).toCompletableFuture().get());
    }

    @Override
    public Set<TopicPartition> topicPartitions(Set<String> topics) {
      return Utils.packException(
          () -> asyncAdmin.topicPartitions(topics).toCompletableFuture().get());
    }

    @Override
    public Set<TopicPartition> topicPartitions(int broker) {
      return Utils.packException(
          () -> asyncAdmin.topicPartitions(broker).toCompletableFuture().get());
    }

    @Override
    public List<Replica> replicas(Set<String> topics) {
      return Utils.packException(() -> asyncAdmin.replicas(topics).toCompletableFuture().get());
    }

    @Override
    public TopicCreator creator() {
      return asyncAdmin.creator();
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
    public Set<String> transactionIds() {
      return Utils.packException(() -> asyncAdmin.transactionIds().toCompletableFuture().get());
    }

    @Override
    public List<Transaction> transactions(Set<String> transactionIds) {
      return Utils.packException(
          () -> asyncAdmin.transactions(transactionIds).toCompletableFuture().get());
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
    public List<AddingReplica> addingReplicas(Set<String> topics) {
      return Utils.packException(
          () -> asyncAdmin.addingReplicas(topics).toCompletableFuture().get());
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
                  replica -> {
                    if (replica.isLeader())
                      leaders.add(
                          TopicPartitionReplica.of(
                              replica.topic(), replica.partition(), replica.nodeInfo().id()));
                    else
                      followers.add(
                          TopicPartitionReplica.of(
                              replica.topic(), replica.partition(), replica.nodeInfo().id()));
                  });
          return this;
        }

        @Override
        public ReplicationThrottler throttle(TopicPartition topicPartition) {
          var replicas =
              replicas(Set.of(topicPartition.topic())).stream()
                  .filter(replica -> replica.partition() == topicPartition.partition())
                  .collect(Collectors.toList());
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
        public AffectedResources apply() {
          applyBandwidth();
          applyThrottledReplicas();
          return new AffectedResources() {
            final Map<Integer, DataRate> ingressCopy = Map.copyOf(ingress);
            final Map<Integer, DataRate> egressCopy = Map.copyOf(egress);
            final Set<TopicPartitionReplica> leaderCopy = Set.copyOf(leaders);
            final Set<TopicPartitionReplica> followerCopy = Set.copyOf(followers);

            @Override
            public Map<Integer, DataRate> ingress() {
              return ingressCopy;
            }

            @Override
            public Map<Integer, DataRate> egress() {
              return egressCopy;
            }

            @Override
            public Set<TopicPartitionReplica> leaders() {
              return leaderCopy;
            }

            @Override
            public Set<TopicPartitionReplica> followers() {
              return followerCopy;
            }
          };
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
          replicas(Set.of(topicPartition.topic())).stream()
              .filter(replica -> replica.partition() == topicPartition.partition())
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
      // Attempt to submit two config alterations might encounter some bug.
      // We have to submit all the changes in one API request.
      // see https://github.com/skiptests/astraea/issues/649
      var configValue = log.partition() + ":" + log.brokerId();
      var configEntry0 = new ConfigEntry("leader.replication.throttled.replicas", configValue);
      var configEntry1 = new ConfigEntry("follower.replication.throttled.replicas", configValue);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, log.topic());
      Utils.packException(
          () ->
              admin.incrementalAlterConfigs(
                  Map.of(
                      configResource,
                      List.of(
                          new AlterConfigOp(configEntry0, AlterConfigOp.OpType.SUBTRACT),
                          new AlterConfigOp(configEntry1, AlterConfigOp.OpType.SUBTRACT)))));
    }

    @Override
    public void clearLeaderReplicationThrottle(TopicPartitionReplica log) {
      var configValue = log.partition() + ":" + log.brokerId();
      var configEntry0 = new ConfigEntry("leader.replication.throttled.replicas", configValue);
      var alterConfigOp0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.SUBTRACT);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, log.topic());
      Utils.packException(
          () -> admin.incrementalAlterConfigs(Map.of(configResource, List.of(alterConfigOp0))));
    }

    @Override
    public void clearFollowerReplicationThrottle(TopicPartitionReplica log) {
      var configValue = log.partition() + ":" + log.brokerId();
      var configEntry1 = new ConfigEntry("follower.replication.throttled.replicas", configValue);
      var alterConfigOp1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.SUBTRACT);
      var configResource = new ConfigResource(ConfigResource.Type.TOPIC, log.topic());
      Utils.packException(
          () -> admin.incrementalAlterConfigs(Map.of(configResource, List.of(alterConfigOp1))));
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

  private static class MigratorImpl implements ReplicaMigrator {
    private final org.apache.kafka.clients.admin.Admin admin;
    private final Function<Set<String>, Set<TopicPartition>> partitionGetter;
    private final Function<Integer, Set<TopicPartition>> brokerPartitionGetter;
    private final Set<TopicPartition> partitions = new HashSet<>();

    MigratorImpl(
        org.apache.kafka.clients.admin.Admin admin,
        Function<Set<String>, Set<TopicPartition>> partitionGetter,
        Function<Integer, Set<TopicPartition>> brokerPartitionGetter) {
      this.admin = admin;
      this.partitionGetter = partitionGetter;
      this.brokerPartitionGetter = brokerPartitionGetter;
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
    public ReplicaMigrator broker(int broker) {
      partitions.addAll(brokerPartitionGetter.apply(broker));
      return this;
    }

    @Override
    public ReplicaMigrator topicOfBroker(int broker, String topic) {
      partitions.addAll(
          brokerPartitionGetter.apply(broker).stream()
              .filter(tp -> Objects.equals(tp.topic(), topic))
              .collect(Collectors.toList()));
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
              .map(org.apache.kafka.common.Node::id)
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
              .map(org.apache.kafka.common.Node::id)
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
