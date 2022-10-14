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
package org.astraea.common.balancer.executor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

@Deprecated
class RebalanceAdminImpl implements RebalanceAdmin {

  private final Admin admin;
  private final AsyncAdmin asyncAdmin;

  /**
   * Construct an implementation of {@link RebalanceAdmin}
   *
   * @param admin the actual {@link Admin} implementation
   */
  public RebalanceAdminImpl(Admin admin) {
    this.admin = admin;
    this.asyncAdmin = (AsyncAdmin) Utils.member(admin, "asyncAdmin");
  }

  /**
   * Declare the preferred data directory at certain brokers.
   *
   * <p>By default, upon a new log creation with JBOD enabled broker. Kafka broker will pick up a
   * data directory that has the fewest log maintained to be the data directory for the new log.
   * This method declares the preferred data directory for a specific topic/partition on the certain
   * broker. Upon the new log creation for the specific topic/partition on the certain broker. The
   * preferred data directory will be used as the data directory for the new log, which replaces the
   * default approach. This gives you the control to decide which data directory the replica log you
   * are about to migrate will be.
   *
   * @param topicPartition the topic/partition to declare preferred data directory
   * @param preferredPlacements the replica placements with their desired data directory at certain
   *     brokers
   */
  private void declarePreferredDataDirectories(
      TopicPartition topicPartition, LinkedHashMap<Integer, String> preferredPlacements) {

    final var currentBrokerAllocation =
        admin.brokers().stream()
            .map(NodeInfo::id)
            .filter(id -> admin.topicPartitions(id).contains(topicPartition))
            .collect(Collectors.toSet());

    // this operation is not supposed to trigger a log movement. But there might be a small window
    // of time to actually trigger it (race condition).
    final var declareMap =
        preferredPlacements.entrySet().stream()
            // filter out offline replicas
            .filter(entry -> entry.getValue() != null)
            .filter(entry -> !currentBrokerAllocation.contains(entry.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .declarePreferredDir(declareMap);
  }

  @Override
  public List<ReplicaMigrationTask> alterReplicaPlacements(
      TopicPartition topicPartition, LinkedHashMap<Integer, String> expectedPlacement) {

    // ensure replica will be placed in the correct data directory at destination broker.
    declarePreferredDataDirectories(topicPartition, expectedPlacement);

    var currentReplicaBrokers =
        admin.brokers().stream()
            .map(NodeInfo::id)
            .filter(id -> admin.topicPartitions(id).contains(topicPartition))
            .collect(Collectors.toSet());

    // do cross broker migration
    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .moveTo(new ArrayList<>(expectedPlacement.keySet()));

    // wait until the whole cluster knows the replica list just changed
    Utils.sleep(Duration.ofMillis(500));

    // do inter-data-directories migration
    var forCrossDirMigration =
        expectedPlacement.entrySet().stream()
            // filter out offline replica
            .filter(entry -> entry.getValue() != null)
            .filter(entry -> currentReplicaBrokers.contains(entry.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .moveTo(forCrossDirMigration);

    return expectedPlacement.keySet().stream()
        .map(s -> TopicPartitionReplica.of(topicPartition.topic(), topicPartition.partition(), s))
        .map(log -> new ReplicaMigrationTask(this, log))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public CompletableFuture<Boolean> waitLogSynced(TopicPartitionReplica log, Duration timeout) {
    return asyncAdmin
        .waitCluster(
            Set.of(log.topic()),
            clusterInfo ->
                clusterInfo
                    .replicaStream()
                    .filter(r -> r.topic().equals(log.topic()))
                    .filter(r -> r.partition() == log.partition())
                    .filter(r -> r.nodeInfo().id() == log.brokerId())
                    .allMatch(r -> r.inSync() && !r.isFuture()),
            timeout,
            2)
        .toCompletableFuture();
  }

  @Override
  public CompletableFuture<Boolean> waitPreferredLeaderSynced(
      TopicPartition topicPartition, Duration timeout) {
    return asyncAdmin
        .waitCluster(
            Set.of(topicPartition.topic()),
            clusterInfo ->
                clusterInfo
                    .replicaStream()
                    .filter(r -> r.topic().equals(topicPartition.topic()))
                    .filter(r -> r.partition() == topicPartition.partition())
                    .filter(Replica::isPreferredLeader)
                    .allMatch(ReplicaInfo::isLeader),
            timeout,
            2)
        .toCompletableFuture();
  }

  @Override
  public LeaderElectionTask leaderElection(TopicPartition topicPartition) {
    admin.preferredLeaderElection(topicPartition);

    return new LeaderElectionTask(this, topicPartition);
  }

  @Override
  public ClusterInfo<Replica> clusterInfo() {
    return admin.clusterInfo();
  }
}
