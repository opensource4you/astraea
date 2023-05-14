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
package org.astraea.common.assignor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Shuffler {
  Map<String, List<TopicPartition>> shuffle();

  static Shuffler randomShuffler(
      Map<String, SubscriptionInfo> subscriptions,
      Map<TopicPartition, Double> partitionCost,
      Map<TopicPartition, Set<TopicPartition>> incompatible,
      Configuration config) {
    Limiter<Long> timeLimiter =
        (s) -> System.currentTimeMillis() - s < config.duration("shuffle.time").get().toMillis();
    Limiter<Map<String, List<TopicPartition>>> unsuitableLimiter =
        (combinator) ->
            combinator.entrySet().stream()
                    .map(
                        e -> {
                          var consumer = e.getKey();
                          var tps = e.getValue();

                          var unsuitable =
                              incompatible.entrySet().stream()
                                  .filter(entry -> tps.contains(entry.getKey()))
                                  .flatMap(entry -> entry.getValue().stream())
                                  .collect(Collectors.toUnmodifiableSet());
                          return Map.entry(
                              consumer, tps.stream().filter(unsuitable::contains).count());
                        })
                    .mapToDouble(Map.Entry::getValue)
                    .sum()
                == 0;
    Limiter<Map<String, List<TopicPartition>>> skewedLimiter =
        (combinator) -> {
          var consumerWithTotalCost =
              combinator.entrySet().stream()
                  .map(
                      e ->
                          Map.entry(
                              e.getKey(),
                              e.getValue().stream().mapToDouble(partitionCost::get).sum()))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          var avg =
              consumerWithTotalCost.values().stream().mapToDouble(d -> d).average().orElse(0.0);
          var standardDeviation =
              Math.sqrt(
                  consumerWithTotalCost.values().stream()
                      .mapToDouble(v -> Math.pow(v - avg, 2))
                      .average()
                      .getAsDouble());
          return standardDeviation < 0.3;
        };

    return () -> {
      var tmpCost =
          subscriptions.keySet().stream()
              .map(c -> Map.entry(c, 0.0))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      var hint = Hint.lowCostHint(subscriptions, tmpCost);
      var generator = Generator.randomGenerator(partitionCost.keySet(), hint);

      Map<String, List<TopicPartition>> result = null;
      var possibleCombinations = new PossibleCombination(partitionCost, incompatible);
      var start = System.currentTimeMillis();

      while (timeLimiter.check(start)) {
        var combinator = generator.get();
        tmpCost.forEach((k, ignore) -> tmpCost.replace(k, 0.0));
        possibleCombinations.add(combinator);
        if (skewedLimiter.check(combinator) && unsuitableLimiter.check(combinator)) {
          result = combinator;
          break;
        }
      }
      return result == null ? possibleCombinations.get() : result;
    };
  }
}
