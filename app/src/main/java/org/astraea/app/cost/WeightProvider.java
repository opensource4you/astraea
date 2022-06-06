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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/** Used to provide the weight to score the node or partition */
@FunctionalInterface
public interface WeightProvider {

  /**
   * create a weight provider based on entropy
   *
   * @param normalizer to normalize input values
   * @return weight for each metrics
   */
  static WeightProvider entropy(Normalizer normalizer) {
    return new EntropyWeightProvider(normalizer);
  }

  /**
   * compute the weights for each metric
   *
   * @param <Metrics> used to represent the "resource". For example, throughput, memory usage, etc.
   * @param <T> used to represent the "target". For example, broker node, topic partition, etc.
   * @param values origin data
   * @return metric and its weight
   */
  <Metrics, T extends Collection<Double>> Map<Metrics, Double> weight(Map<Metrics, T> values);

  class EntropyWeightProvider implements WeightProvider {
    private final Normalizer normalizer;

    EntropyWeightProvider(Normalizer normalizer) {
      this.normalizer = normalizer;
    }

    @Override
    public <Metrics, T extends Collection<Double>> Map<Metrics, Double> weight(
        Map<Metrics, T> values) {
      var entropies = entropies(values);
      // use difference to calculate the weight
      var diff =
          entropies.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> 1D - e.getValue()));
      var sum = diff.values().stream().mapToDouble(d -> d).sum();
      return diff.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() / sum));
    }

    // used for testing
    <Metric, T extends Collection<Double>> Map<Metric, Double> entropies(Map<Metric, T> values) {
      return values.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e ->
                      normalizer.normalize(e.getValue()).stream()
                              // return 0 if value is 0 (just convenience of calculation)
                              .mapToDouble(value -> value * (value == 0 ? 0 : Math.log(value)))
                              .sum()
                          / (-Math.log(e.getValue().size()))));
    }
  }
}
