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
package org.astraea.app.partitioner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * a way to pick best target.
 *
 * @param <E> target type
 */
public interface RoundRobin<E> {

  static <E> RoundRobin<E> smoothWeighted(Map<E, Double> weights) {
    return new SmoothWeightedRoundRobin<>(weights);
  }

  /**
   * calculate the next object to use
   *
   * @param availableTargets available targets
   * @return obj to send, or null if this algorithm fails to get answer for current condition.
   */
  Optional<E> next(Set<E> availableTargets);

  class SmoothWeightedRoundRobin<E> implements RoundRobin<E> {

    private final Map<E, Double> effectiveWeights;
    private volatile Map<E, Double> currentWeights;

    private SmoothWeightedRoundRobin(Map<E, Double> weights) {
      this.effectiveWeights = Collections.unmodifiableMap(weights);
      this.currentWeights =
          weights.keySet().stream()
              .collect(Collectors.toUnmodifiableMap(Function.identity(), ignored -> 0D));
    }

    @Override
    public Optional<E> next(Set<E> availableTargets) {
      // no data no answer
      if (effectiveWeights.isEmpty() || availableTargets.isEmpty()) return Optional.empty();

      // 1) calculate the sum of all effective weights
      var sum = effectiveWeights.values().stream().mapToDouble(d -> d).sum();
      // 2) add effective weight to each current weight
      var nextWeights =
          currentWeights.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e -> effectiveWeights.getOrDefault(e.getKey(), 0D) + e.getValue()));
      // 3) get the E which has max value
      var maxObj =
          nextWeights.entrySet().stream()
              .filter(e -> availableTargets.contains(e.getKey()))
              .max(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey);
      // 4) subtract weight from max (effective weight)
      maxObj.ifPresent(
          o -> {
            nextWeights.put(o, nextWeights.get(o) - sum);
            // 5) update current weights by next weights
            currentWeights = Collections.unmodifiableMap(nextWeights);
          });
      return maxObj;
    }
  }
}
