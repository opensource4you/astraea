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

import static org.astraea.common.admin.ClusterInfo.findNonFulfilledAllocation;

import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  private final boolean enableDataDirectoryMigration;

  public StraightPlanExecutor(Configuration configuration) {
    this.enableDataDirectoryMigration =
        configuration
            .string(StraightPlanExecutor.CONFIG_ENABLE_DATA_DIRECTORY_MIGRATION)
            .map(Boolean::parseBoolean)
            .orElse(false);
  }

  @Override
  public CompletionStage<Void> run(Admin admin, ClusterInfo logAllocation, Duration timeout) {
    return admin
        .topicNames(true)
        .thenCompose(admin::clusterInfo)
        .thenApply(
            clusterInfo -> {
              if (clusterInfo
                  .replicaStream()
                  .anyMatch(r -> r.isFuture() || r.isRemoving() || r.isAdding()))
                throw new IllegalArgumentException(
                    "There are moving replicas. Stop re-balance plan");
              return findNonFulfilledAllocation(clusterInfo, logAllocation);
            })
        .thenApply(
            tps ->
                tps.stream()
                    .flatMap(tp -> logAllocation.replicas(tp).stream())
                    .collect(Collectors.toList()))
        // step 0: declare preferred data dir
        .thenCompose(
            replicas ->
                admin
                    .declarePreferredDataFolders(
                        replicas.stream()
                            .collect(
                                Collectors.toMap(Replica::topicPartitionReplica, Replica::path)))
                    .thenApply((ignore) -> replicas))
        // step 1: move replicas to specify brokers
        .thenCompose(
            replicas ->
                admin
                    .moveToBrokers(
                        replicas.stream()
                            .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
                            .collect(
                                Collectors.groupingBy(
                                    Replica::topicPartition,
                                    Collectors.mapping(r -> r.brokerId(), Collectors.toList()))))
                    .thenApply(ignored -> replicas))
        // step 2: wait replicas get reassigned
        .thenCompose(
            replicas ->
                admin
                    .waitCluster(
                        logAllocation.topicNames(),
                        clusterInfo ->
                            clusterInfo
                                .topicPartitionReplicas()
                                .containsAll(logAllocation.topicPartitionReplicas()),
                        timeout,
                        5)
                    .thenApply(
                        done -> {
                          if (!done)
                            throw new IllegalStateException(
                                "Failed to move "
                                    + replicas.stream()
                                        .map(Replica::topicPartitionReplica)
                                        .collect(Collectors.toSet()));
                          return replicas;
                        }))
        // step.3 move replicas to specify folders
        .thenCompose(
            replicas ->
                enableDataDirectoryMigration
                    ? admin.moveToFolders(
                        replicas.stream()
                            .collect(
                                Collectors.toMap(Replica::topicPartitionReplica, Replica::path)))
                    : CompletableFuture.completedFuture(null))
        // step.4 wait replicas get synced
        .thenCompose(
            replicas -> admin.waitReplicasSynced(logAllocation.topicPartitionReplicas(), timeout))
        // step.5 re-elect leaders
        .thenCompose(
            topicPartitions ->
                admin
                    .preferredLeaderElection(logAllocation.topicPartitions())
                    .thenCompose(
                        ignored ->
                            admin.waitPreferredLeaderSynced(
                                logAllocation.topicPartitions(), timeout))
                    .thenAccept(c -> assertion(c, "Failed to re-election for " + topicPartitions)));
  }

  static void assertion(boolean condition, String info) {
    if (!condition) throw new IllegalStateException(info);
  }
}
