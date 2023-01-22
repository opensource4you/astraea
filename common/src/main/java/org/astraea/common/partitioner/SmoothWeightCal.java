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
package org.astraea.common.partitioner;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.cost.Dispersion;

public final class SmoothWeightCal<E> {
  private final double UPPER_LIMIT_OFFSET_RATIO = 0.1;
  private final Dispersion dispersion = Dispersion.standardDeviation();
  private Map<E, Double> currentEffectiveWeightResult;
  Supplier<Map<E, Double>> effectiveWeightResult;

  SmoothWeightCal(Map<E, Double> effectiveWeight) {
    this.effectiveWeightResult =
        () ->
            effectiveWeight.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, ignored -> 1.0));
    this.currentEffectiveWeightResult = effectiveWeightResult.get();
  }

  /**
   * Update effective weight.
   *
   * @param brokerScore Broker Score.
   */
  synchronized void refresh(Supplier<Map<E, Double>> brokerScore) {
    this.effectiveWeightResult =
        () -> {
          var score = brokerScore.get();
          var avgScore = score.values().stream().mapToDouble(i -> i).average().orElse(1.0);
          var offsetRatioOfBroker =
              score.entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey, entry -> (entry.getValue() - avgScore) / avgScore));
          // If the average offset of all brokers from the cluster is greater than 0.1, it is
          // unbalanced.
          var balance =
              dispersion.calculate(new ArrayList<>(score.values()))
                  > UPPER_LIMIT_OFFSET_RATIO * avgScore;
          this.currentEffectiveWeightResult =
              this.currentEffectiveWeightResult.entrySet().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Map.Entry::getKey,
                          entry -> {
                            var offsetRatio = offsetRatioOfBroker.get(entry.getKey());
                            var weight =
                                balance ? entry.getValue() * (1 - offsetRatio) : entry.getValue();
                            return Math.max(weight, 0.1);
                          }));

          return this.currentEffectiveWeightResult;
        };
  }
}
