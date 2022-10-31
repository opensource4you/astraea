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

  @Override
  public CompletionStage<Void> run(Admin admin, ClusterLogAllocation logAllocation) {
    return admin
        .topicNames(true)
        .thenCompose(admin::clusterInfo)
        .thenApply(clusterInfo -> findNonFulfilledAllocation(clusterInfo, logAllocation))
        .thenApply(
            topicPartitions ->
                topicPartitions.stream()
                    .map(
                        tp ->
                            logAllocation.replicas(tp).stream()
                                .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
                                .collect(Collectors.toUnmodifiableList()))
                    .map(
                        replicaList ->
                            admin
                                .moveToBrokers(toReplicaMap(replicaList))
                                .thenCompose(i -> waitStart(admin, replicaList))
                                .thenCompose(i -> admin.moveToFolders(toPathMap(replicaList)))
                                .thenCompose(
                                    i ->
                                        admin.waitReplicasSynced(
                                            replicaList.stream()
                                                .map(ReplicaInfo::topicPartitionReplica)
                                                .collect(Collectors.toSet()),
                                            ChronoUnit.DECADES.getDuration()))
                                .thenAccept(c -> assertion(c, "Failed to sync " + replicaList))
                                .thenCompose(
                                    i ->
                                        admin.preferredLeaderElection(
                                            Set.of(replicaList.get(0).topicPartition())))
                                .thenCompose(
                                    i ->
                                        admin.waitPreferredLeaderSynced(
                                            Set.of(replicaList.get(0).topicPartition()),
                                            ChronoUnit.DECADES.getDuration()))
                                .thenAccept(c -> assertion(c, "Failed to sync " + replicaList)))
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new))
        .thenCompose(CompletableFuture::allOf);
  }

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

  private CompletionStage<Boolean> waitStart(Admin admin, List<Replica> replicas) {
    return admin.waitCluster(
        Set.of(replicas.get(0).topic()),
        (cluster) ->
            replicas.stream()
                .allMatch(
                    r ->
                        cluster.replicas(r.topicPartition()).stream()
                            .anyMatch(rr -> rr.nodeInfo().id() == r.nodeInfo().id())),
        Duration.ofSeconds(5),
        3);
  }
}
