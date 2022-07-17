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
package org.astraea.app.balancer.executor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.Utils;

class RebalanceAdminImpl implements RebalanceAdmin {

  private final Predicate<String> topicFilter;
  private final Admin admin;

  /**
   * Construct an implementation of {@link RebalanceAdmin}
   *
   * @param topicFilter to determine which topics are permitted for balance operation
   * @param admin the actual {@link Admin} implementation
   */
  public RebalanceAdminImpl(Predicate<String> topicFilter, Admin admin) {
    this.topicFilter = topicFilter;
    this.admin = admin;
  }

  private void ensureTopicPermitted(String topic) {
    if (!topicFilter.test(topic))
      throw new IllegalArgumentException("Operation to topic \"" + topic + "\" is not permitted");
  }

  private List<LogPlacement> fetchCurrentPlacement(TopicPartition topicPartition) {
    ensureTopicPermitted(topicPartition.topic());
    return admin.replicas(Set.of(topicPartition.topic())).get(topicPartition).stream()
        .map(replica -> LogPlacement.of(replica.broker(), replica.path()))
        .collect(Collectors.toUnmodifiableList());
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
      TopicPartition topicPartition, List<LogPlacement> preferredPlacements) {
    ensureTopicPermitted(topicPartition.topic());

    final var currentPlacement = fetchCurrentPlacement(topicPartition);

    final var currentBrokerAllocation =
        currentPlacement.stream().map(LogPlacement::broker).collect(Collectors.toUnmodifiableSet());

    // this operation is not supposed to trigger a log movement. But there might be a small window
    // of time to actually trigger it (race condition).
    final var declareMap =
        preferredPlacements.stream()
            .filter(futurePlacement -> !currentBrokerAllocation.contains(futurePlacement.broker()))
            .filter(futurePlacement -> futurePlacement.logDirectory().isPresent())
            .collect(
                Collectors.toUnmodifiableMap(
                    LogPlacement::broker, x -> x.logDirectory().orElseThrow()));

    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .declarePreferredDir(declareMap);
  }

  @Override
  public List<ReplicaMigrationTask> alterReplicaPlacements(
      TopicPartition topicPartition, List<LogPlacement> expectedPlacement) {
    ensureTopicPermitted(topicPartition.topic());

    // ensure replica will be placed in the correct data directory at destination broker.
    declarePreferredDataDirectories(topicPartition, expectedPlacement);

    var currentReplicaBrokers =
        fetchCurrentPlacement(topicPartition).stream()
            .map(LogPlacement::broker)
            .collect(Collectors.toUnmodifiableSet());

    // do cross broker migration
    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .moveTo(
            expectedPlacement.stream()
                .map(LogPlacement::broker)
                .collect(Collectors.toUnmodifiableList()));
    // do inter-data-directories migration
    var forCrossDirMigration =
        expectedPlacement.stream()
            .filter(placement -> currentReplicaBrokers.contains(placement.broker()))
            .filter(placement -> placement.logDirectory().isPresent())
            .collect(
                Collectors.toUnmodifiableMap(
                    LogPlacement::broker, placement -> placement.logDirectory().orElseThrow()));
    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .moveTo(forCrossDirMigration);

    return expectedPlacement.stream()
        .map(
            log ->
                TopicPartitionReplica.of(
                    topicPartition.topic(), topicPartition.partition(), log.broker()))
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
    ensureTopicPermitted(log.topic());

    return CompletableFuture.supplyAsync(
        () -> {
          // due to the state consistency issue in Kafka broker design. the cluster state returned
          // from the API might bounce between the `old state` and the `new state` during the very
          // beginning and accomplishment of the cluster state alteration API. to fix this we use
          // debounce technique, to ensure the target condition is held over a few successive tries,
          // which mean the cluster state alteration is considered stable.
          var debounce = 2;
          var endTime = getEndTime(timeout);
          while (!Thread.currentThread().isInterrupted()) {
            boolean synced =
                admin.replicas(Set.of(log.topic())).entrySet().stream()
                    .filter(x -> x.getKey().partition() == log.partition())
                    .filter(x -> x.getKey().topic().equals(log.topic()))
                    .flatMap(x -> x.getValue().stream())
                    .filter(x -> x.broker() == log.brokerId())
                    .findFirst()
                    .map(x -> x.inSync() && !x.isFuture())
                    .orElse(false);
            // debounce & retrial interval
            Utils.sleep(retrialTime.get());
            debounce = synced ? (debounce - 1) : 2;
            // synced
            if (synced && debounce <= 0) return true;
            // timeout
            if (System.currentTimeMillis() > endTime) return false;
          }
          return false;
        });
  }

  @Override
  public CompletableFuture<Boolean> waitPreferredLeaderSynced(
      TopicPartition topicPartition, Duration timeout) {
    ensureTopicPermitted(topicPartition.topic());

    return CompletableFuture.supplyAsync(
        () -> {
          // due to the state consistency issue in Kafka broker design. the cluster state returned
          // from the API might bounce between the `old state` and the `new state` during the very
          // beginning and accomplishment of the cluster state alteration API. to fix this we use
          // debounce technique, to ensure the target condition is held over a few successive tries,
          // which mean the cluster state alteration is considered stable.
          var debounce = 2;
          var endTime = getEndTime(timeout);
          while (!Thread.currentThread().isInterrupted()) {
            var synced =
                admin.replicas(Set.of(topicPartition.topic())).entrySet().stream()
                    .filter(x -> x.getKey().equals(topicPartition))
                    .findFirst()
                    .map(Map.Entry::getValue)
                    .map(
                        replicas -> {
                          var preferred =
                              replicas.stream()
                                  .filter(Replica::isPreferredLeader)
                                  .findFirst()
                                  .orElseThrow();
                          return preferred.leader();
                        })
                    .orElseThrow();
            // debounce & retrial interval
            Utils.sleep(retrialTime.get());
            debounce = synced ? (debounce - 1) : 2;
            // synced
            if (synced && debounce <= 0) return true;
            // timeout
            if (System.currentTimeMillis() > endTime) return false;
          }
          return false;
        });
  }

  @Override
  public LeaderElectionTask leaderElection(TopicPartition topicPartition) {
    ensureTopicPermitted(topicPartition.topic());

    admin.preferredLeaderElection(topicPartition);

    return new LeaderElectionTask(this, topicPartition);
  }

  @Override
  public ClusterInfo clusterInfo() {
    return admin.clusterInfo(
        admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet()));
  }

  @Override
  public Predicate<String> topicFilter() {
    return topicFilter;
  }

  private static final AtomicReference<Duration> retrialTime =
      new AtomicReference<>(Duration.ofSeconds(1));

  // visible for test
  static void changeRetrialTime(Duration newDebounceTime) {
    retrialTime.set(newDebounceTime);
  }
}
