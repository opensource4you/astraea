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

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Shuffler {
  /**
   * Try to avoid putting incompatible partitions on the same consumer.
   *
   * @param subscriptions the subscription of consumers
   * @param assignment assignment
   * @param incompatible incompatible partition calculated by cost function
   * @param costs partition cost
   * @return assignment that filter out most incompatible partitions
   */
  Map<String, List<TopicPartition>> shuffle(
      Map<String, SubscriptionInfo> subscriptions,
      Map<String, List<TopicPartition>> assignment,
      Map<TopicPartition, Set<TopicPartition>> incompatible,
      Map<TopicPartition, Double> costs);

  static Shuffler incompatible(Duration maxTime) {
    return (subscriptions, assignment, incompatible, costs) -> {
      if (incompatible.isEmpty()) return assignment;
      // get the incompatible partitions of each consumer from consumer assignment
      var unsuitable =
          assignment.entrySet().stream()
              .map(
                  e ->
                      Map.entry(
                          e.getKey(),
                          e.getValue().stream()
                              .filter(incompatible::containsKey)
                              .flatMap(tp -> incompatible.get(tp).stream())
                              .collect(Collectors.toUnmodifiableSet())))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      if (assignment.entrySet().stream()
          .noneMatch(
              e -> e.getValue().stream().anyMatch(tp -> unsuitable.get(e.getKey()).contains(tp))))
        return assignment;

      var possibleAssignments = new HashSet<Map<String, List<TopicPartition>>>();
      var randomAssign =
          (Function<TopicPartition, String>)
              (tp) -> {
                var subsConsumer =
                    subscriptions.entrySet().stream()
                        .filter(e -> e.getValue().topics().contains(tp.topic()))
                        .toList();
                return subsConsumer
                    .get(ThreadLocalRandom.current().nextInt(subsConsumer.size()))
                    .getKey();
              };

      var start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < maxTime.toMillis()) {
        possibleAssignments.add(
            costs.keySet().stream()
                .map(tp -> Map.entry(randomAssign.apply(tp), tp))
                .collect(
                    Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList()))));
      }

      var standardSigma =
          (Function<Map<String, List<TopicPartition>>, Double>)
              (r) -> {
                var totalCost =
                    r.entrySet().stream()
                        .map(
                            e ->
                                Map.entry(
                                    e.getKey(),
                                    e.getValue().stream().mapToDouble(costs::get).sum()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                var avg = totalCost.values().stream().mapToDouble(d -> d).average().getAsDouble();

                return Math.sqrt(
                    totalCost.values().stream().mapToDouble(c -> Math.pow(avg - c, 2)).sum()
                        / totalCost.size());
              };
      var numberOfIncompatibility =
          (Function<Map<String, List<TopicPartition>>, Integer>)
              (possibleAssignment) -> {
                var unsuit =
                    possibleAssignment.entrySet().stream()
                        .map(
                            e ->
                                Map.entry(
                                    e.getKey(),
                                    e.getValue().stream()
                                        .flatMap(tp -> incompatible.get(tp).stream())
                                        .collect(Collectors.toUnmodifiableSet())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                return possibleAssignment.entrySet().stream()
                    .mapToInt(
                        e ->
                            (int)
                                e.getValue().stream()
                                    .filter(tp -> unsuit.get(e.getKey()).contains(tp))
                                    .count())
                    .sum();
              };

      return possibleAssignments.stream()
          .map(e -> Map.entry(e, standardSigma.apply(e)))
          .sorted(Map.Entry.comparingByValue())
          .map(Map.Entry::getKey)
          .limit((int) Math.floor((double) possibleAssignments.size() / 10))
          .min(Comparator.comparingLong(numberOfIncompatibility::apply))
          .get();
    };
  }
}
