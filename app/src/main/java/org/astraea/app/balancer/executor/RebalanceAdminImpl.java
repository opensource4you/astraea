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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.metrics.HasBeanObject;

class RebalanceAdminImpl implements RebalanceAdmin {

  private final Predicate<String> topicFilter;
  private final Admin admin;
  private final Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource;

  /**
   * Construct a implementation of {@link RebalanceAdmin}
   *
   * @param topicFilter to determine which topics are permitted for balance operation
   * @param admin the actual {@link Admin} implementation
   * @param metricSource the supplier for new metrics, this supplier should return the metrics that
   *     {@link RebalancePlanExecutor#fetcher()} is interested.
   */
  public RebalanceAdminImpl(
      Predicate<String> topicFilter,
      Admin admin,
      Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource) {
    this.topicFilter = topicFilter;
    this.admin = admin;
    this.metricSource = metricSource;
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

    // TODO: this operation is not supposed to trigger a log movement. But there might be a
    // small window of time to actually trigger it (race condition).
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
        .moveTo(declareMap);
  }

  @Override
  public List<ReplicaMigrationTask> alterReplicaPlacements(
      TopicPartition topicPartition, List<LogPlacement> expectedPlacement) {
    ensureTopicPermitted(topicPartition.topic());

    // ensure replica will be placed in the correct data directory at destination broker.
    declarePreferredDataDirectories(topicPartition, expectedPlacement);

    // do cross broker migration
    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .moveTo(
            expectedPlacement.stream()
                .map(LogPlacement::broker)
                .collect(Collectors.toUnmodifiableList()));
    // do inter-data-directories migration
    admin
        .migrator()
        .partition(topicPartition.topic(), topicPartition.partition())
        .moveTo(
            expectedPlacement.stream()
                .filter(placement -> placement.logDirectory().isPresent())
                .collect(
                    Collectors.toUnmodifiableMap(
                        LogPlacement::broker, x -> x.logDirectory().orElseThrow())));

    return expectedPlacement.stream()
        .map(
            log ->
                new TopicPartitionReplica(
                    topicPartition.topic(), topicPartition.partition(), log.broker()))
        .map(log -> new ReplicaMigrationTask(this, log))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public SyncingProgress syncingProgress(TopicPartitionReplica log) {
    this.ensureTopicPermitted(log.topic());

    List<Replica> replicas =
        admin.replicas(Set.of(log.topic())).entrySet().stream()
            .filter(x -> x.getKey().partition() == log.partition())
            .filter(x -> x.getKey().topic().equals(log.topic()))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow();

    var topicPartition = new TopicPartition(log.topic(), log.partition());
    var leader = replicas.stream().filter(Replica::leader).findFirst();
    var replica =
        replicas.stream().filter(x -> x.broker() == log.brokerId()).findFirst().orElseThrow();

    return leader
        .map(leaderLog -> SyncingProgress.of(topicPartition, replica, leaderLog))
        .orElse(SyncingProgress.leaderlessProgress(topicPartition, replica));
  }

  @Override
  public boolean waitLogSynced(TopicPartitionReplica log, Duration timeout)
      throws InterruptedException {
    ensureTopicPermitted(log.topic());
    return await(
        () ->
            admin.replicas(Set.of(log.topic())).entrySet().stream()
                .filter(x -> x.getKey().partition() == log.partition())
                .filter(x -> x.getKey().topic().equals(log.topic()))
                .flatMap(x -> x.getValue().stream())
                .filter(x -> x.broker() == log.brokerId())
                .findFirst()
                .map(Replica::inSync)
                .orElse(false),
        timeout);
  }

  @Override
  public boolean waitPreferredLeaderSynced(TopicPartition topicPartition, Duration timeout)
      throws InterruptedException {
    ensureTopicPermitted(topicPartition.topic());
    // the set of interested topics.
    return await(
        () ->
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
                .orElseThrow(),
        timeout);
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
  public ClusterInfo refreshMetrics(ClusterInfo oldClusterInfo) {
    return ClusterInfo.of(oldClusterInfo, metricSource.get());
  }

  static boolean await(Supplier<Boolean> task, Duration timeout) throws InterruptedException {
    long retryInterval = Duration.ofSeconds(0).toMillis();
    Function<Long, Long> nextRetry = (current) -> Math.min(5000, current + 1000);
    long nowMs = System.currentTimeMillis();
    long oldMs = nowMs;
    long timeoutMs = timeout.getSeconds() * 1000 + timeout.toMillisPart() + nowMs;

    // overflow detection
    if (timeoutMs < timeout.getSeconds()) timeoutMs = Long.MAX_VALUE;

    boolean isDone;
    do {
      TimeUnit.MILLISECONDS.sleep(Math.max(0, retryInterval - (nowMs - oldMs)));

      oldMs = nowMs;
      isDone = task.get();
      retryInterval = nextRetry.apply(retryInterval);
      nowMs = System.currentTimeMillis();
    } while (!isDone && timeoutMs > nowMs);

    return isDone;
  }
}
