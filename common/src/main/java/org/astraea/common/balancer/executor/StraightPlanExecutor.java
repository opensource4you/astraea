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
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public CompletionStage<Void> submit(
      Admin admin, ClusterInfo<Replica> logAllocation, Duration timeout) {
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
            replicas -> {
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
                              timeout,
                              1))
                  .thenAccept(
                      done ->
                          RebalancePlanExecutor.assertion(
                              done, "Failed to move " + moveFolderRequest.keySet()))
                  // step.3 move replica to specify folder
                  .thenCompose(ignored -> admin.moveToFolders(moveFolderRequest));
            });
  }
}
