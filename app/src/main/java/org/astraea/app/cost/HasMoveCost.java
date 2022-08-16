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
package org.astraea.app.cost;

import java.util.Map;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;

public interface HasMoveCost extends CostFunction {
  /**
   * score migrate cost from originClusterInfo to newClusterInfo .
   *
   * @param originClusterInfo the clusterInfo before migrate
   * @param newClusterInfo the mocked clusterInfo generate from balancer
   * @param clusterBean cluster metrics
   * @return the score of migrate cost
   */
  ClusterCost clusterCost(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean);

  /**
   * @param originClusterInfo the clusterInfo before migrate
   * @param newClusterInfo the mocked clusterInfo generate from balancer
   * @param clusterBean cluster metrics
   * @return Check if the migrate plan exceeds the available hardware resources
   */
  boolean overflow(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean);

  /**
   * @param originClusterInfo the clusterInfo before migrate
   * @param newClusterInfo the mocked clusterInfo generate from balancer
   * @param clusterBean cluster metrics
   * @return total migrate size of the plan
   */
  Map<MoveCost.ReplicaMigrateInfo, Long> totalMigrateSize(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean);

  /**
   * @param originClusterInfo the clusterInfo before migrate
   * @param newClusterInfo the mocked clusterInfo generate from balancer
   * @param clusterBean cluster metrics
   * @return estimated migration time of the plan
   */
  double estimatedMigrateTime(
      ClusterInfo originClusterInfo, ClusterInfo newClusterInfo, ClusterBean clusterBean);
}
