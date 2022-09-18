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

import java.util.Collection;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

@FunctionalInterface
public interface HasMoveCost extends CostFunction {
  /**
   * score migrate cost from originClusterInfo to newClusterInfo
   *
   * @param before the clusterInfo before migrate
   * @param after the mocked clusterInfo generate from balancer
   * @param clusterBean cluster metrics
   * @return the score of migrate cost
   */
  MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean);

  /**
   * Use this helper when you need to implement the calculation method of total cost or broker
   * changes by yourself
   */
  interface Helper extends HasMoveCost {

    default MoveCost moveCost(
        ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
      return moveCost(
          ClusterInfo.diff(before, after), ClusterInfo.diff(after, before), clusterBean);
    }

    /**
     * @param removedReplicas replicas removed from the source broker
     * @param addedReplicas replicas removed add to the sink broker
     * @param clusterBean cluster metrics
     * @return the score of migrate cost
     */
    MoveCost moveCost(
        Collection<Replica> removedReplicas,
        Collection<Replica> addedReplicas,
        ClusterBean clusterBean);
  }
}
