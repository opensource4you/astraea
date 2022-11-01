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
import java.util.concurrent.CompletionStage;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

/** This class associate with the logic of fulfill given rebalance plan. */
public interface RebalancePlanExecutor {
  /**
   * submit the migration request to servers. Noted that this method get completed when all requests
   * are accepted. The data syncing will keep running on the server side. Hence, there is no
   * guarantee that the leader re-election is invoked ( Kafka requires re-election can happen only
   * if related replicas are get synced). Please call {@link RebalancePlanExecutor#execute(Admin,
   * ClusterInfo, Duration)} to add extra check/wait for data sync and re-election
   *
   * @param admin to process request
   * @param targetAllocation the expected assignments
   * @param timeout to wait metadata sync for each phase. The plan could be divided into many
   *     requests. This timeout could stop the infinite waiting for Kafka metadata. It causes
   *     IllegalStateException if the metadata can't get synced and timeout is expired.
   * @return a background running thread
   */
  CompletionStage<Void> submit(
      Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout);

  /**
   * this method compose of {@link RebalancePlanExecutor#submit(Admin, ClusterInfo, Duration)} and
   * data sync waiting. It makes sure all migrated replicas get synced and all new leaders get
   * ready. The timeout to wait sync and re-election can be "large" and "unpredictable", so it is up
   * to caller to give a "suitable" timeout.
   *
   * @param admin to process request
   * @param targetAllocation the expected assignments
   * @param timeout to wait metadata sync for each phase. The plan could be divided into many
   *     requests. This timeout could stop the infinite waiting for Kafka metadata. It causes
   *     IllegalStateException if the metadata can't get synced and timeout is expired.
   * @return a background running thread
   */
  default CompletionStage<Void> execute(
      Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
    return submit(admin, targetAllocation, timeout)
        .thenCompose(
            replicas ->
                admin.waitReplicasSynced(targetAllocation.topicPartitionReplicas(), timeout))
        // step.3 re-elect leaders
        .thenCompose(
            topicPartitions ->
                admin
                    .preferredLeaderElection(targetAllocation.topicPartitions())
                    .thenCompose(
                        ignored ->
                            admin.waitPreferredLeaderSynced(
                                targetAllocation.topicPartitions(), timeout))
                    .thenAccept(c -> assertion(c, "Failed to re-election for " + topicPartitions)));
  }

  static void assertion(boolean condition, String info) {
    if (!condition) throw new IllegalStateException(info);
  }
}
