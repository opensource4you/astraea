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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Limiter {

  boolean check(Map<String, List<TopicPartition>> condition);

  static Limiter of(Set<Limiter> limiters) {
    return (combinator) -> limiters.stream().allMatch(l -> l.check(combinator));
  }

  static Limiter incompatibleLimiter(Map<TopicPartition, Set<TopicPartition>> incompatible) {
    return (combinator) ->
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
                      return Map.entry(consumer, tps.stream().filter(unsuitable::contains).count());
                    })
                .mapToDouble(Map.Entry::getValue)
                .sum()
            == 0;
  }

  static Limiter skewCostLimiter(
      Map<TopicPartition, Double> partitionCost, Map<String, SubscriptionInfo> subscriptions) {
    var tmpConsumerCost =
        subscriptions.keySet().stream()
            .map(c -> Map.entry(c, 0.0))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    partitionCost.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .map(Map.Entry::getKey)
        .forEach(
            tp -> {
              var minCostConsumer =
                  tmpConsumerCost.entrySet().stream().min(Map.Entry.comparingByValue()).get();
              minCostConsumer.setValue(minCostConsumer.getValue() + partitionCost.get(tp));
            });
    var standardDeviation =
        (Function<Collection<Double>, Double>)
            (vs) -> {
              var average = vs.stream().mapToDouble(c -> c).average().getAsDouble();
              return Math.sqrt(
                  vs.stream().mapToDouble(v -> Math.pow(v - average, 2)).average().getAsDouble());
            };
    var limit = standardDeviation.apply(tmpConsumerCost.values());

    return (combinator) -> {
      var sd =
          standardDeviation.apply(
              combinator.values().stream()
                  .map(tps -> tps.stream().mapToDouble(partitionCost::get).sum())
                  .collect(Collectors.toSet()));

      return sd < limit;
    };
  }
}
