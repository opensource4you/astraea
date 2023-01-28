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
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.function.Bi3Function;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricSensor;

@FunctionalInterface
public interface HasClusterCost extends CostFunction {

  static HasClusterCost of(Map<HasClusterCost, Double> costAndWeight) {
    var fetcher =
        Fetcher.of(
            costAndWeight.keySet().stream()
                .map(CostFunction::fetcher)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()));
    var sensors =
        costAndWeight.keySet().stream()
            .flatMap(x -> x.sensors().stream())
            .collect(Collectors.toList());

    return new HasClusterCost() {
      @Override
      public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
        var scores =
            costAndWeight.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getKey().clusterCost(clusterInfo, clusterBean).value()));
        var totalWeight = costAndWeight.values().stream().mapToDouble(x -> x).sum();
        var compositeScore =
            costAndWeight.keySet().stream()
                .mapToDouble(cost -> scores.get(cost) * costAndWeight.get(cost) / totalWeight)
                .sum();

        return new ClusterCost() {
          @Override
          public double value() {
            return compositeScore;
          }

          @Override
          public String toString() {
            Bi3Function<HasClusterCost, Double, Double, String> descriptiveName =
                (function, cost, weight) ->
                    "{\"" + function.toString() + "\" cost " + cost + " weight " + weight + "}";
            return "WeightCompositeClusterCost["
                + costAndWeight.entrySet().stream()
                    .sorted(
                        Comparator.<Map.Entry<HasClusterCost, Double>>comparingDouble(
                                Map.Entry::getValue)
                            .reversed())
                    .map(
                        e ->
                            descriptiveName.apply(e.getKey(), scores.get(e.getKey()), e.getValue()))
                    .collect(Collectors.joining(", "))
                + "] = "
                + compositeScore;
          }
        };
      }

      @Override
      public Optional<Fetcher> fetcher() {
        return fetcher;
      }

      @Override
      public Collection<MetricSensor> sensors() {
        return sensors;
      }

      @Override
      public String toString() {
        BiFunction<HasClusterCost, Double, String> descriptiveName =
            (cost, value) -> "{\"" + cost.toString() + "\" weight " + value + "}";
        return "WeightCompositeClusterCostFunction["
            + costAndWeight.entrySet().stream()
                .sorted(
                    Comparator.<Map.Entry<HasClusterCost, Double>>comparingDouble(
                            Map.Entry::getValue)
                        .reversed())
                .map(e -> descriptiveName.apply(e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "))
            + "]";
      }
    };
  }

  /**
   * score cluster for a particular metrics according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @param clusterBean cluster metrics
   * @return the score of cluster.
   */
  ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean);
}
