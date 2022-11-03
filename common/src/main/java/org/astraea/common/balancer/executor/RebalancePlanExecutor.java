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

  static RebalancePlanExecutor of() {
    return new StraightPlanExecutor();
  }

  /**
   * submit the migration request to servers. It makes sure all migrated replicas get synced and all
   * new leaders get ready. The timeout to wait sync and re-election can be "large" and
   * "unpredictable", so it is up to caller to give a "suitable" timeout.
   *
   * @param admin to process request
   * @param targetAllocation the expected assignments
   * @param timeout to wait metadata sync for each phase. The plan could be divided into many
   *     requests. This timeout could stop the infinite waiting for Kafka metadata. It causes
   *     IllegalStateException if the metadata can't get synced and timeout is expired.
   * @return a background running thread
   */
  CompletionStage<Void> run(Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout);
}
