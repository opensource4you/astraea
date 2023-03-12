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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.metrics.collector.MetricSensor;

/**
 * It is meaningless to implement this interface. Instead, we should implement interfaces like
 * {@link HasBrokerCost} ,{@link HasClusterCost}.
 *
 * <p>Constructors in CostFunction can only take no or only one parameter {@link Configuration}, ex.
 * Constructor({@link Configuration} configuration) or Constructor()
 *
 * <p>A cost function might take advantage of some Mbean metrics to function. One can indicate such
 * requirement in the {@link CostFunction#metricSensor()} function. If the operation logic of
 * CostFunction thinks the metrics on hand are insufficient or not ready to work. One can throw a
 * {@link NoSufficientMetricsException} from the calculation logic of {@link HasBrokerCost} or
 * {@link HasClusterCost}. This serves as a hint to the caller that it needs newer metrics and
 * please retry later.
 *
 * <p>Also, it is recommended to override {@link Object#toString()} to provide the descriptive text
 * along the cost function instance. This information might be shown on some user interfaces.
 */
public interface CostFunction {

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  default Optional<MetricSensor> metricSensor() {
    return Optional.empty();
  }

  static String toStringComposite(Map<? extends CostFunction, Double> costWeights) {
    BiFunction<CostFunction, Double, String> descriptiveName =
        (cost, value) -> "{\"" + cost.toString() + "\" weight " + value + "}";
    return "["
        + costWeights.entrySet().stream()
            .sorted(
                Comparator.<Map.Entry<?, Double>>comparingDouble(Map.Entry::getValue).reversed())
            .map(e -> descriptiveName.apply(e.getKey(), e.getValue()))
            .collect(Collectors.joining(", "))
        + "]";
  }
}
