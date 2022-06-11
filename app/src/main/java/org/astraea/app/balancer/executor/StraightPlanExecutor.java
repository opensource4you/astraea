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

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {
    try {
      final var clusterInfo = context.rebalanceAdmin().clusterInfo();
      final var currentLogAllocation = LayeredClusterLogAllocation.of(clusterInfo);
      final var migrationTargets =
          ClusterLogAllocation.findNonFulfilledAllocation(
              context.expectedAllocation(), currentLogAllocation);

      var executeReplicaMigration =
          (Function<TopicPartition, List<RebalanceTask<TopicPartitionReplica, SyncingProgress>>>)
              (topicPartition) ->
                  context
                      .rebalanceAdmin()
                      .alterReplicaPlacements(
                          topicPartition,
                          context.expectedAllocation().logPlacements(topicPartition));

      // do log migration
      migrationTargets.stream()
          .map(executeReplicaMigration)
          .flatMap(Collection::stream)
          .forEach(
              task -> {
                if (!task.await()) throw new IllegalStateException("Log should be synced");
              });

      // do leader election
      migrationTargets.stream()
          .map(tp -> context.rebalanceAdmin().leaderElection(tp))
          .forEach(
              task -> {
                if (!task.await())
                  throw new IllegalStateException("Preferred leader should be the leader");
              });

      return RebalanceExecutionResult.done();
    } catch (Exception e) {
      return RebalanceExecutionResult.failed(e);
    }
  }
}
