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
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.balancer.log.ClusterLogAllocation;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  private Map<TopicPartitionReplica, String> toPathMap(List<Replica> replicas) {
    return replicas.stream()
        .collect(Collectors.toMap(ReplicaInfo::topicPartitionReplica, Replica::path));
  }

  private Map<TopicPartition, List<Integer>> toReplicaMap(List<Replica> replicas) {
    return Map.of(
        replicas.get(0).topicPartition(),
        replicas.stream().map(Replica::nodeInfo).map(NodeInfo::id).collect(Collectors.toList()));
  }

  private void assertion(boolean condition, String info) {
    if (!condition) throw new IllegalStateException(info);
  }

  @Override
  public CompletionStage<Void> run(Admin admin, ClusterLogAllocation logAllocation) {
    return admin
        .topicNames(true)
        .thenCompose(admin::clusterInfo)
        .thenApply(
            clusterInfo ->
                ClusterLogAllocation.findNonFulfilledAllocation(clusterInfo, logAllocation))
        .thenCompose(
            topicPartitions -> {
              var allMigrations =
                  topicPartitions.stream()
                      .map(
                          tp ->
                              logAllocation.logPlacements(tp).stream()
                                  .sorted(
                                      Comparator.comparing(Replica::isPreferredLeader).reversed())
                                  .collect(Collectors.toUnmodifiableList()))
                      .map(
                          replicaList ->
                              admin
                                  // perform broker migration
                                  .moveToBrokers(toReplicaMap(replicaList))
                                  // wait until the cluster knows the replica list is changed
                                  .thenCompose(
                                      i ->
                                          admin.waitCluster(
                                              Set.of(replicaList.get(0).topic()),
                                              (cluster) ->
                                                  cluster.replicas().stream()
                                                      .anyMatch(x -> !x.inSync()),
                                              Duration.ofSeconds(5),
                                              3))
                                  // perform folder migration
                                  .thenCompose(i -> admin.moveToFolders(toPathMap(replicaList)))
                                  // wait until the migration finished
                                  .thenCompose(
                                      i ->
                                          admin.waitReplicasSynced(
                                              replicaList.stream()
                                                  .map(ReplicaInfo::topicPartitionReplica)
                                                  .collect(Collectors.toSet()),
                                              ChronoUnit.DECADES.getDuration()))
                                  .thenAccept(
                                      done -> assertion(done, "Failed to sync " + replicaList))
                                  // perform preferred leader election
                                  .thenCompose(
                                      i ->
                                          admin.preferredLeaderElection(
                                              Set.of(replicaList.get(0).topicPartition())))
                                  // wait until the preferred leader is ready
                                  .thenCompose(
                                      i ->
                                          admin.waitPreferredLeaderSynced(
                                              Set.of(replicaList.get(0).topicPartition()),
                                              ChronoUnit.DECADES.getDuration()))
                                  .thenAccept(
                                      done -> assertion(done, "Failed to sync " + replicaList)))
                      .map(CompletionStage::toCompletableFuture)
                      .collect(Collectors.toList());
              return CompletableFuture.allOf(allMigrations.toArray(CompletableFuture[]::new));
            });
  }
}
