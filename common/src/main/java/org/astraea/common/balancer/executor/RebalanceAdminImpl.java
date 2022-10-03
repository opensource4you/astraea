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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

class RebalanceAdminImpl implements RebalanceAdmin {

  private final Admin admin;

  /**
   * Construct an implementation of {@link RebalanceAdmin}
   *
   * @param admin the actual {@link Admin} implementation
   */
  public RebalanceAdminImpl(Admin admin) {
    this.admin = admin;
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

  private long getEndTime(Duration timeout) {
    try {
      return Math.addExact(System.currentTimeMillis(), timeout.toMillis());
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  @Override
  public CompletableFuture<Boolean> waitLogSynced(TopicPartitionReplica log, Duration timeout) {
    return CompletableFuture.supplyAsync(
        debounceCheck(
            timeout,
            () ->
                admin.replicas(Set.of(log.topic())).stream()
                    .filter(x -> x.topic().equals(log.topic()))
                    .filter(x -> x.partition() == log.partition())
                    .filter(x -> x.nodeInfo().id() == log.brokerId())
                    .findFirst()
                    .map(x -> x.inSync() && !x.isFuture())
                    .orElse(false)));
  }

  @Override
  public CompletableFuture<Boolean> waitPreferredLeaderSynced(
      TopicPartition topicPartition, Duration timeout) {
    return CompletableFuture.supplyAsync(
        debounceCheck(
            timeout,
            () ->
                admin.replicas(Set.of(topicPartition.topic())).stream()
                    .filter(x -> x.topic().equals(topicPartition.topic()))
                    .filter(x -> x.partition() == topicPartition.partition())
                    .findFirst()
                    .filter(Replica::isPreferredLeader)
                    .map(ReplicaInfo::isLeader)
                    .orElseThrow()));
  }

  private Supplier<Boolean> debounceCheck(Duration timeout, Supplier<Boolean> testDone) {
    var debounceInitialCount = 10;
    return () -> {
      // due to the state consistency issue in Kafka broker design. the cluster state returned
      // from the API might bounce between the `old state` and the `new state` during the very
      // beginning and accomplishment of the cluster state alteration API. to fix this we use
      // debounce technique, to ensure the target condition is held over a few successive tries,
      // which mean the cluster state alteration is considered stable.
      var debounce = debounceInitialCount;
      var endTime = getEndTime(timeout);
      while (!Thread.currentThread().isInterrupted()) {
        // debounce & retrial interval
        Utils.sleep(Duration.ofMillis(100));
        var isDone = testDone.get();
        debounce = isDone ? (debounce - 1) : debounceInitialCount;
        // synced
        if (isDone && debounce <= 0) return true;
        // timeout
        if (System.currentTimeMillis() > endTime) return false;
      }
      return false;
    };
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
