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
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.collector.MetricSensor;

@FunctionalInterface
public interface HasBrokerCost extends CostFunction {

  HasBrokerCost EMPTY = (clusterInfo, clusterBean) -> Map::of;

  static HasBrokerCost of(Map<HasBrokerCost, Double> costAndWeight) {
    // the temporary exception won't affect the smooth-weighted too much.
    // TODO: should we propagate the exception by better way? For example: Slf4j ?
    // see https://github.com/skiptests/astraea/issues/486
    var sensor =
        MetricSensor.of(
            costAndWeight.keySet().stream()
                .map(CostFunction::metricSensor)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()));
    return new HasBrokerCost() {
      @Override
      public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
        var merged = new HashMap<Integer, Double>();
        costAndWeight.forEach(
            (f, w) ->
                Normalizer.DEFAULT
                    .normalize(f.brokerCost(clusterInfo, clusterBean).value())
                    .forEach(
                        (i, v) ->
                            merged.compute(
                                i,
                                (ignored, previous) ->
                                    previous == null ? v * w : v * w + previous)));
        var result = Normalizer.DEFAULT.normalize(merged);
        return () -> result;
      }

      @Override
      public Optional<MetricSensor> metricSensor() {
        return sensor;
      }

      @Override
      public String toString() {
        return "WeightCompositeBrokerCostFunction" + CostFunction.toStringComposite(costAndWeight);
      }
    };
  }

  /**
   * score all nodes for a particular metrics according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @param clusterBean cluster metrics
   * @return the score of each broker.
   */
  BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean);
}
