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

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.common.admin.ClusterInfo;

@FunctionalInterface
public interface RebalancePlanGenerator {

  static RebalancePlanGenerator random(int numberOfShuffle) {
    return new ShufflePlanGenerator(() -> numberOfShuffle);
  }

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * <p>If the generator implementation thinks it can't find any rebalance proposal(which the plan
   * might improve the cluster). Then the implementation should return a Stream with exactly one
   * rebalance plan proposal in it, where the proposed allocation will be exactly the same as the
   * {@code baseAllocation} parameter. This means there is no movement or alteration that will
   * occur. And The implementation should place some detailed information in the info/warning/error
   * string, to indicate the reason for no meaningful plan.
   *
   * @param brokerFolders key is the broker id, and the value is the folder used to keep data
   * @param baseAllocation the cluster log allocation as the based of proposal generation.
   * @return a {@link Stream} generating rebalance plan regarding the given {@link ClusterInfo}
   */
  Stream<RebalancePlanProposal> generate(
      Map<Integer, Set<String>> brokerFolders, ClusterLogAllocation baseAllocation);
}
