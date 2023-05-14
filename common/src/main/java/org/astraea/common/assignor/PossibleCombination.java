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
import org.astraea.common.admin.TopicPartition;

public class PossibleCombination {
  private final Set<Map<String, List<TopicPartition>>> possibleCombinations;
  private final Map<TopicPartition, Double> partitionCost;
  private final Map<TopicPartition, Set<TopicPartition>> incompatible;

  PossibleCombination(
      Map<TopicPartition, Double> partitionCost,
      Map<TopicPartition, Set<TopicPartition>> incompatible) {
    this.possibleCombinations = new HashSet<>();
    this.partitionCost = partitionCost;
    this.incompatible = incompatible;
  }

  void add(Map<String, List<TopicPartition>> combinator) {
    possibleCombinations.add(combinator);
  }

  Map<String, List<TopicPartition>> get() {
    var standardSigma =
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

    var candidates =
        possibleCombinations.stream()
            .map(combinator -> Map.entry(combinator, standardSigma.apply(combinator)))
            .sorted(Map.Entry.comparingByValue())
            .limit((int) Math.ceil(possibleCombinations.size() / 10.0))
            .map(e -> Map.entry(incompatibility.apply(e.getKey()), e.getKey()))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toSet())))
            .entrySet()
            .stream()
            .min(Map.Entry.comparingByKey())
            .get()
            .getValue();

    return candidates.stream()
        .map(c -> Map.entry(standardSigma.apply(c), c))
        .min(Map.Entry.comparingByKey())
        .get()
        .getValue();
  }
}
