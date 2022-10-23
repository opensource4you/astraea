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

import java.util.concurrent.CompletionStage;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.balancer.log.ClusterLogAllocation;

/** This class associate with the logic of fulfill given rebalance plan. */
public interface RebalancePlanExecutor {

  /** This method responsible for fulfill a rebalance plan. */
  void run(RebalanceAdmin rebalanceAdmin, ClusterLogAllocation targetAllocation);

  CompletionStage<Void> run(AsyncAdmin admin, ClusterLogAllocation targetAllocation);
}
