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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
    var timeLimiter = Limiter.timeLimiter(config);
    var skewLimiter = Limiter.skewCostLimiter(partitionCost, subscriptions);
    var incompatibleLimiter = Limiter.incompatibleLimiter(incompatible);
    var hints =
        Hint.of(
            Set.of(
                Hint.lowCostHint(subscriptions, partitionCost),
                Hint.incompatibleHint(subscriptions, incompatible)));
    var standardDeviation =
        (Function<Map<String, List<TopicPartition>>, Double>)
            (combinator) -> {
              var totalCost =
                  combinator.entrySet().stream()
                      .map(
                          e ->
                              Map.entry(
                                  e.getKey(),
                                  e.getValue().stream().mapToDouble(partitionCost::get).sum()))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

              var avg = totalCost.values().stream().mapToDouble(d -> d).average().getAsDouble();
              return Math.sqrt(
                  totalCost.values().stream().mapToDouble(cost -> Math.pow(avg - cost, 2)).sum());
            };
    var incompatibility =
        (Function<Map<String, List<TopicPartition>>, Integer>)
            (combinator) -> {
              var unsuitablePerConsumer =
                  combinator.entrySet().stream()
                      .map(
                          e ->
                              Map.entry(
                                  e.getKey(),
                                  e.getValue().stream()
                                      .flatMap(tp -> incompatible.get(tp).stream())
                                      .collect(Collectors.toUnmodifiableSet())))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

              return combinator.entrySet().stream()
                  .mapToInt(
                      e ->
                          (int)
                              e.getValue().stream()
                                  .filter(tp -> unsuitablePerConsumer.get(e.getKey()).contains(tp))
                                  .count())
                  .sum();
            };
    var rejectedCombinators = new HashSet<Map<String, List<TopicPartition>>>();

    return () -> {
      var generator =
          Generator.randomGenerator(subscriptions.keySet(), partitionCost.keySet(), hints);
      Map<String, List<TopicPartition>> result = null;
      var start = System.currentTimeMillis();

      while (timeLimiter.check(start)) {
        var combinator = generator.get();
        rejectedCombinators.add(combinator);
        if (skewLimiter.check(combinator) && incompatibleLimiter.check(combinator)) {
          result = combinator;
          break;
        }
      }

      if (result == null) {
        var combinators =
            rejectedCombinators.stream()
                .map(c -> Map.entry(c, standardDeviation.apply(c)))
                .sorted(Map.Entry.comparingByValue())
                .toList();

        result =
            combinators.stream()
                .limit((long) Math.ceil(combinators.size() / 10.0))
                .map(e -> Map.entry(e.getKey(), incompatibility.apply(e.getKey())))
                .min(Map.Entry.comparingByValue())
                .get()
                .getKey();
      }
      return result;
    };
  }
}
