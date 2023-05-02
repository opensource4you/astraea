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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.collector.MetricSensor;

@FunctionalInterface
public interface HasPartitionCost extends CostFunction {

  HasPartitionCost EMPTY = (clusterInfo, clusterBean) -> Map::of;

  static HasPartitionCost of(Map<HasPartitionCost, Double> costAndWeight) {
    var sensor =
        MetricSensor.of(
            costAndWeight.keySet().stream()
                .map(CostFunction::metricSensor)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()));
    return new HasPartitionCost() {
      @Override
      public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
        var partitionCost =
            costAndWeight.keySet().stream()
                .map(f -> Map.entry(f, f.partitionCost(clusterInfo, clusterBean)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        return new PartitionCost() {
          @Override
          public Map<TopicPartition, Double> value() {
            var aggregatedCost = new HashMap<TopicPartition, Double>();
            partitionCost.forEach(
                (f, cost) ->
                    cost.value()
                        .forEach(
                            (tp, v) ->
                                aggregatedCost.compute(
                                    tp,
                                    (ignore, previous) ->
                                        previous == null
                                            ? v * costAndWeight.get(f)
                                            : v * costAndWeight.get(f) + previous)));
            return aggregatedCost;
          }

          @Override
          public Map<TopicPartition, Set<TopicPartition>> incompatibility() {
            var incompatibleSet =
                partitionCost.values().stream()
                    .map(PartitionCost::incompatibility)
                    .flatMap(m -> m.entrySet().stream())
                    .collect(
                        Collectors.groupingBy(
                            Map.Entry::getKey,
                            Collectors.flatMapping(
                                e -> e.getValue().stream(), Collectors.toUnmodifiableSet())));
            return incompatibleSet;
          }
        };
      }

      @Override
      public Optional<MetricSensor> metricSensor() {
        return sensor;
      }

      @Override
      public String toString() {
        return "WeightCompositePartitionCostFunction"
            + CostFunction.toStringComposite(costAndWeight);
      }
    };
  }

  PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean);
}
