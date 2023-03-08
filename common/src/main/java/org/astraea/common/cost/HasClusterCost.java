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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.function.Bi3Function;
import org.astraea.common.metrics.collector.MetricSensor;

@FunctionalInterface
public interface HasClusterCost extends CostFunction {

  static HasClusterCost of(Map<HasClusterCost, Double> costAndWeight) {
    var sensor =
        MetricSensor.of(
            costAndWeight.keySet().stream()
                .map(CostFunction::metricSensor)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()));

    return new HasClusterCost() {
      @Override
      public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
        var scores =
            costAndWeight.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, e -> e.getKey().clusterCost(clusterInfo, clusterBean)));
        var totalWeight = costAndWeight.values().stream().mapToDouble(x -> x).sum();
        var compositeScore =
            costAndWeight.keySet().stream()
                .mapToDouble(
                    cost -> scores.get(cost).value() * costAndWeight.get(cost) / totalWeight)
                .sum();

        return ClusterCost.of(
            compositeScore,
            () -> {
              Bi3Function<HasClusterCost, ClusterCost, Double, String> descriptiveName =
                  (function, cost, weight) ->
                      "{\""
                          + function.toString()
                          + "\" cost "
                          + cost.value()
                          + " weight "
                          + weight
                          + " description "
                          + cost
                          + " }";
              return "WeightCompositeClusterCost["
                  + costAndWeight.entrySet().stream()
                      .sorted(Map.Entry.<HasClusterCost, Double>comparingByValue().reversed())
                      .map(
                          e ->
                              descriptiveName.apply(
                                  e.getKey(), scores.get(e.getKey()), e.getValue()))
                      .collect(Collectors.joining(", "))
                  + "] = "
                  + compositeScore;
            });
      }

      @Override
      public Optional<MetricSensor> metricSensor() {
        return sensor;
      }

      @Override
      public String toString() {
        return "WeightCompositeClusterCostFunction" + CostFunction.toStringComposite(costAndWeight);
      }
    };
  }

  /**
   * score cluster for a particular metrics according to passed beans and cluster information. Note
   * that when ClusterInfo and ClusterBean have similar information, you must first refer to
   * ClusterInfo, The main reason is that the balancer will use the estimated migration distribution
   * to calculate the ClusterCost when generating the plan, and ClusterBean will only return the
   * metrics of the actual cluster, so in most of the time, ClusterInfo can be used to estimate the
   * state after migration. But some costs need to be calculated using ClusterBean, for example:
   * {@link NetworkCost}
   *
   * @param clusterInfo cluster information
   * @param clusterBean cluster metrics
   * @return the score of cluster.
   */
  ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean);
}
