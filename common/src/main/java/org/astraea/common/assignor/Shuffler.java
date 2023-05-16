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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

public interface Shuffler {
  Map<String, List<TopicPartition>> shuffle();

  Map<String, List<TopicPartition>> fairlyCombinator();

  static Shuffler randomShuffler(
      Map<String, SubscriptionInfo> subscriptions,
      Map<TopicPartition, Double> partitionCost,
      Map<TopicPartition, Set<TopicPartition>> incompatible) {
    var limiters =
        Limiter.of(
            Set.of(
                Limiter.skewCostLimiter(partitionCost, subscriptions),
                Limiter.incompatibleLimiter(incompatible)));
    var generator = Generator.randomGenerator(subscriptions, partitionCost, incompatible);
    var standardDeviation =
        (Function<Map<String, List<TopicPartition>>, Double>)
            (combinator) -> {
              var costPerConsumer =
                  combinator.entrySet().stream()
                      .map(
                          e ->
                              Map.entry(
                                  e.getKey(),
                                  e.getValue().stream().mapToDouble(partitionCost::get).sum()))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              var avg =
                  costPerConsumer.values().stream().mapToDouble(d -> d).average().getAsDouble();

              return Math.sqrt(
                  costPerConsumer.values().stream()
                      .mapToDouble(d -> Math.pow(d - avg, 2))
                      .average()
                      .getAsDouble());
            };
    return new Shuffler() {
      final Set<Map<String, List<TopicPartition>>> rejectedCombinators = new HashSet<>();

      @Override
      public Map<String, List<TopicPartition>> shuffle() {
        var combinator = generator.get();

        if (limiters.check(combinator)) return combinator;
        rejectedCombinators.add(combinator);

        return Map.of();
      }

      @Override
      public Map<String, List<TopicPartition>> fairlyCombinator() {
        if (rejectedCombinators.isEmpty())
          throw new NoSuchElementException("there is no element in rejected combinators");

        return rejectedCombinators.stream()
            .map(c -> Map.entry(c, standardDeviation.apply(c)))
            .min(Map.Entry.comparingByValue())
            .get()
            .getKey();
      }
    };
  }
}
