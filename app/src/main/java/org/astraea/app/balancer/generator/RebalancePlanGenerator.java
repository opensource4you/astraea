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
package org.astraea.app.balancer.generator;

import java.util.stream.Stream;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.log.ClusterLogAllocation;

/** */
public interface RebalancePlanGenerator {

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * @param clusterInfo the cluster state, implementation can take advantage of the data inside to
   *     proposal the plan it feels confident to improve the cluster.
   * @return a {@link Stream} generating rebalance plan regarding the given {@link ClusterInfo}
   */
  default Stream<RebalancePlanProposal> generate(ClusterInfo clusterInfo) {
    return generate(clusterInfo, ClusterLogAllocation.of(clusterInfo));
  }

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * @param clusterInfo the cluster state, implementation can take advantage of the data inside to
   *     proposal the plan it feels confident to improve the cluster.
   * @param baseAllocation the cluster log allocation as the based of proposal generation.
   * @return a {@link Stream} generating rebalance plan regarding the given {@link ClusterInfo}
   */
  Stream<RebalancePlanProposal> generate(
      ClusterInfo clusterInfo, ClusterLogAllocation baseAllocation);
}
