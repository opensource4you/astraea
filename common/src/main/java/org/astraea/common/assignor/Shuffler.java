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

public interface Shuffler {
  Map<String, List<TopicPartition>> shuffle();

  static Shuffler randomShuffler(
      Map<String, SubscriptionInfo> subscriptions,
      Map<TopicPartition, Double> partitionCost,
      Map<TopicPartition, Set<TopicPartition>> incompatible,
      Configuration config) {
    var limiters =
        Limiter.of(
            Set.of(
                Limiter.skewCostLimiter(partitionCost), Limiter.incompatibleLimiter(incompatible)));
    var hints =
        Hint.of(
            Set.of(
                Hint.lowCostHint(subscriptions, partitionCost),
                Hint.incompatibleHint(subscriptions, incompatible)));
    var generator = Generator.randomGenerator(subscriptions, partitionCost, hints);
    var shuffleTime = config.duration("shuffle.time").get().toMillis();
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
    var rejectedCombinators = new HashSet<Map<String, List<TopicPartition>>>();

    return () -> {
      Map<String, List<TopicPartition>> result = null;
      var start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < shuffleTime) {
        var combinator = generator.get();
        if (limiters.check(combinator)) {
          result = combinator;
          break;
        }
        rejectedCombinators.add(combinator);
      }
      return result == null
          ? rejectedCombinators.stream()
              .map(c -> Map.entry(c, standardDeviation.apply(c)))
              .min(Map.Entry.comparingByValue())
              .get()
              .getKey()
          : result;
    };
  }
}
