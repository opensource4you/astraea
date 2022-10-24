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
package org.astraea.common.balancer.algorithms;

import java.time.Duration;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

/** The generic algorithm parameter for resolving the Kafka rebalance problem. */
public interface AlgorithmConfig {

  /** @return the cluster cost function for this problem. */
  HasClusterCost clusterCostFunction();

  /** @return the movement cost functions for this problem */
  List<HasMoveCost> moveCostFunctions();

  /** @return the cluster cost constraint that must be complied with by the algorithm solution */
  BiPredicate<ClusterCost, ClusterCost> clusterCostConstraint();

  /** @return the movement constraint that must be complied with by the algorithm solution */
  Predicate<List<MoveCost>> movementConstraint();

  /** @return the limit of algorithm execution time */
  Duration executionTime();

  /** @return a {@link Supplier} which offer the fresh state of metrics */
  Supplier<ClusterBean> metricSource();

  /** @return the algorithm implementation specific parameters */
  Configuration algorithmConfig();
}
