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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

/**
 * The wrapper of {@link Admin}. Offer only the essential functionalities & some utilities to
 * perform rebalance operation.
 */
public interface RebalanceAdmin {

  /**
   * Construct an implementation of {@link RebalanceAdmin}
   *
   * @param topicFilter to determine which topics are permitted for balance operation
   * @param admin the actual {@link Admin} implementation
   */
  static RebalanceAdmin of(Admin admin, Predicate<String> topicFilter) {
    return new RebalanceAdminImpl(topicFilter, admin);
  }

  /**
   * Attempt to migrate the target topic/partition to the given replica log placement state. This
   * method will perform both replica list migration and data directory migration. This method will
   * return after triggering the migration. It won't wait until the migration processes are
   * fulfilled.
   *
   * @param topicPartition the topic/partition to perform migration
   * @param expectedPlacement the expected placement after this request accomplished
   * @return a list of task trackers regarding each log
   */
  List<ReplicaMigrationTask> alterReplicaPlacements(
      TopicPartition topicPartition, List<LogPlacement> expectedPlacement);

  /** @return a {@link CompletableFuture} that indicate the specific log has become synced. */
  CompletableFuture<Boolean> waitLogSynced(TopicPartitionReplica log, Duration timeout);

  /** @return a {@link CompletableFuture} that indicate the specific log has become synced. */
  default CompletableFuture<Boolean> waitLogSynced(TopicPartitionReplica log) {
    return waitLogSynced(log, ChronoUnit.FOREVER.getDuration());
  }

  /**
   * @return a {@link CompletableFuture} that indicate the specific topic/partition has its
   *     preferred leader becomes the actual leader.
   */
  CompletableFuture<Boolean> waitPreferredLeaderSynced(
      TopicPartition topicPartition, Duration timeout);

  /**
   * @return a {@link CompletableFuture} that indicate the specific topic/partition has its
   *     preferred leader becomes the actual leader.
   */
  default CompletableFuture<Boolean> waitPreferredLeaderSynced(TopicPartition topicPartition) {
    return waitPreferredLeaderSynced(topicPartition, ChronoUnit.FOREVER.getDuration());
  }

  /**
   * Perform preferred leader election for specific topic/partition.
   *
   * @param topicPartition the topic/partition to trigger preferred leader election
   * @return a task tracker to track the election progress.
   */
  LeaderElectionTask leaderElection(TopicPartition topicPartition);

  ClusterInfo<Replica> clusterInfo();

  /**
   * @return a {@link Predicate<String>} indicate which topic name is allowed to operate by this
   *     {@link RebalanceAdmin}.
   */
  Predicate<String> topicFilter();

  // TODO: add method to apply reassignment bandwidth throttle.
  // TODO: add method to fetch topic configuration
  // TODO: add method to fetch broker configuration
}
