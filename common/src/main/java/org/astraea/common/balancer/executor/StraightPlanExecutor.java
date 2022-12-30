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
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  private final boolean disableDataDirectoryMigration;

  public StraightPlanExecutor() {
    this(false);
  }

  public StraightPlanExecutor(boolean disableDataDirectoryMigration) {
    this.disableDataDirectoryMigration = disableDataDirectoryMigration;
  }

  @Override
  public CompletionStage<Void> run(
      Admin admin, ClusterInfo<Replica> logAllocation, Duration timeout) {
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
        // step 1: move replicas to specify brokers
        .thenCompose(
            replicas ->
                admin
                    .moveToBrokers(
                        replicas.stream()
                            .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
                            .collect(
                                Collectors.groupingBy(
                                    ReplicaInfo::topicPartition,
                                    Collectors.mapping(
                                        r -> r.nodeInfo().id(), Collectors.toList()))))
                    .thenApply(ignored -> replicas))
        // step 2: wait replicas get reassigned
        .thenCompose(
            replicas ->
                admin
                    .waitCluster(
                        logAllocation.topics(),
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
                                        .map(ReplicaInfo::topicPartitionReplica)
                                        .collect(Collectors.toSet()));
                          return replicas;
                        }))
        // step.3 move replicas to specify folders
        .thenCompose(
            replicas -> {
              // temporarily disable data-directory migration, there are some Kafka bug related to
              // it.
              // see https://github.com/skiptests/astraea/issues/1325#issue-1506582838
              if (disableDataDirectoryMigration) return CompletableFuture.completedFuture(null);
              else
                return admin.moveToFolders(
                    replicas.stream()
                        .collect(
                            Collectors.toMap(ReplicaInfo::topicPartitionReplica, Replica::path)));
            })
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
