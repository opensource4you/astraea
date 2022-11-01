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
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
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
            tps ->
                tps.stream()
                    .flatMap(tp -> logAllocation.replicas(tp).stream())
                    .sorted(Comparator.comparing(Replica::isPreferredLeader))
                    .collect(Collectors.toList()))

        // step.1 move replica to specify brokers/folders
        .thenCompose(
            replicas ->
                run(admin, replicas)
                    .thenApply(
                        ignored ->
                            replicas.stream()
                                .map(Replica::topicPartitionReplica)
                                .collect(Collectors.toSet())))

        // step.2 wait all replicas to be synced
        .thenCompose(
            replicas ->
                admin
                    .waitReplicasSynced(replicas, ChronoUnit.DECADES.getDuration())
                    .thenApply(
                        ignored ->
                            replicas.stream()
                                .map(TopicPartitionReplica::topicPartition)
                                .collect(Collectors.toSet())))
        // step.3 re-elect leaders
        .thenCompose(
            topicPartitions ->
                admin
                    .preferredLeaderElection(topicPartitions)
                    .thenCompose(
                        ignored ->
                            admin.waitPreferredLeaderSynced(
                                topicPartitions, ChronoUnit.DECADES.getDuration()))
                    .thenAccept(c -> assertion(c, "Failed to sync " + topicPartitions)));
  }

  public CompletionStage<Void> run(Admin admin, List<Replica> replicas) {
    var moveBrokerRequest =
        replicas.stream()
            .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
            .collect(
                Collectors.groupingBy(
                    ReplicaInfo::topicPartition,
                    Collectors.mapping(r -> r.nodeInfo().id(), Collectors.toList())));
    var moveFolderRequest =
        replicas.stream()
            .collect(Collectors.toMap(ReplicaInfo::topicPartitionReplica, Replica::path));
    return admin
        // step.1 move replica to specify brokers
        .moveToBrokers(moveBrokerRequest)
        // step.2 wait assignment
        .thenCompose(
            ignored ->
                admin.waitCluster(
                    moveBrokerRequest.keySet().stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toSet()),
                    clusterInfo ->
                        replicas.stream()
                            .allMatch(
                                r ->
                                    clusterInfo
                                        .replicaStream()
                                        .anyMatch(
                                            replica ->
                                                replica
                                                    .topicPartitionReplica()
                                                    .equals(r.topicPartitionReplica()))),
                    Duration.ofSeconds(15),
                    2))
        .thenAccept(done -> assertion(done, "Failed to sync " + moveFolderRequest.keySet()))
        // step.3 move replica to specify folder
        .thenCompose(ignored -> admin.moveToFolders(moveFolderRequest));
  }

  private void assertion(boolean condition, String info) {
    if (!condition) throw new IllegalStateException(info);
  }
}
