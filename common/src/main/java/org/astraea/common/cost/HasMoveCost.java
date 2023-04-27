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
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.collector.MetricSensor;

@FunctionalInterface
public interface HasMoveCost extends CostFunction {

  HasMoveCost EMPTY = (originClusterInfo, newClusterInfo, clusterBean) -> MoveCost.EMPTY;

  static HasMoveCost of(Collection<HasMoveCost> hasMoveCosts) {
    var sensor =
        MetricSensor.of(
            hasMoveCosts.stream()
                .map(CostFunction::metricSensor)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()));
    return new HasMoveCost() {

      @Override
      public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
        var moveCosts =
            hasMoveCosts.stream()
                .map(c -> c.moveCost(before, after, clusterBean))
                .collect(Collectors.toSet());
        var overflow = moveCosts.stream().anyMatch(MoveCost::overflow);
        return () -> overflow;
      }

      @Override
      public Optional<MetricSensor> metricSensor() {
        return sensor;
      }

      @Override
      public String toString() {
        return "MoveCosts["
            + hasMoveCosts.stream()
                .map(cost -> "\"" + cost.toString() + "\"")
                .collect(Collectors.joining(", "))
            + "]";
      }
    };
  }

  /**
   * score migrate cost from originClusterInfo to newClusterInfo
   *
   * @param before the clusterInfo before migrate
   * @param after the mocked clusterInfo generate from balancer
   * @param clusterBean cluster metrics
   * @return the score of migrate cost
   */
  MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean);
}
