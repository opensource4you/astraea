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
package org.astraea.common.cost;

import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

public class DecreasingCost implements HasClusterCost {

  private ClusterInfo<Replica> original;

  public DecreasingCost(Configuration configuration) {}

  private double value0 = 1.0;

  @Override
  public synchronized ClusterCost clusterCost(
      ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    if (original == null) original = clusterInfo;
    if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
    double theCost = value0;
    value0 = value0 * 0.998;
    return () -> theCost;
  }
}
