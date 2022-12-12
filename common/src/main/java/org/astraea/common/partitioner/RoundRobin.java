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

  static <E> RoundRobin<E> smooth(Map<E, Double> scores) {
    return new SmoothRoundRobin<>(scores);
  }

  /**
   * calculate the next object to use
   *
   * @param availableTargets available targets
   * @return obj to send, or null if this algorithm fails to get answer for current condition.
   */
  Optional<E> next(Set<E> availableTargets);

  class SmoothRoundRobin<E> implements RoundRobin<E> {

    private final Map<E, Double> effectiveScores;

    private final double sum;
    private volatile Map<E, Double> currentScores;

    private SmoothRoundRobin(Map<E, Double> scores) {
      // The effective score should not be zero
      if (scores.values().stream().anyMatch(score -> score <= 0.0)) {
        throw new IllegalArgumentException(
            "Effective score in smooth round-robin should not be zero");
      }
      this.effectiveScores = Collections.unmodifiableMap(scores);
      this.currentScores =
          scores.keySet().stream()
              .collect(Collectors.toUnmodifiableMap(Function.identity(), ignored -> 0D));
      this.sum = effectiveScores.values().stream().mapToDouble(d -> d).sum();
    }

    @Override
    public Optional<E> next(Set<E> availableTargets) {
      // no data no answer
      if (effectiveScores.isEmpty() || availableTargets.isEmpty()) return Optional.empty();

      // 1) add effective score to each current score
      var nextScores =
          currentScores.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e -> effectiveScores.getOrDefault(e.getKey(), 0D) + e.getValue()));
      // 2) get the E which has max value
      var maxObj =
          nextScores.entrySet().stream()
              .filter(e -> availableTargets.contains(e.getKey()))
              .max(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey);
      // 4) subtract score from max (effective score)
      maxObj.ifPresent(
          o -> {
            nextScores.put(o, nextScores.get(o) - sum);
            // 5) update current scores by next scores
            currentScores = Collections.unmodifiableMap(nextScores);
          });
      return maxObj;
    }
  }
}
