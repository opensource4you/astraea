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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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

  static Shuffler incompatible(long maxTime) {
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
                              .flatMap(tp -> incompatible.get(tp).stream())
                              .collect(Collectors.toUnmodifiableSet())))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      System.out.println("unsuitable = " + unsuitable);
      System.out.println("assignment = " + assignment);
      if (assignment.entrySet().stream()
          .noneMatch(
              e -> e.getValue().stream().anyMatch(tp -> unsuitable.get(e.getKey()).contains(tp)))) {
        System.out.println("none match");
        return assignment;
      }

      var tmpCost = new HashMap<>(costs);
      var submitHeavyCost =
          (Function<String, TopicPartition>)
              (c) -> {
                var tpCost =
                    tmpCost.entrySet().stream()
                        .filter(tc -> subscriptions.get(c).topics().contains(tc.getKey().topic()))
                        .max(Map.Entry.comparingByValue())
                        .get();
                tmpCost.remove(tpCost);
                return tpCost.getKey();
              };
      var result =
          subscriptions.keySet().stream()
              .map(c -> Map.entry(c, new ArrayList<TopicPartition>()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      result.forEach((c, r) -> r.add(submitHeavyCost.apply(c)));

      var possibleAssignments = new HashSet<Map<String, List<TopicPartition>>>();
      var randomAssign =
          (Function<TopicPartition, String>)
              (tp) -> {
                var subsConsumer =
                    subscriptions.entrySet().stream()
                        .filter(e -> e.getValue().topics().contains(tp.topic()))
                        .collect(Collectors.toUnmodifiableList());
                return subsConsumer
                    .get(ThreadLocalRandom.current().nextInt(subsConsumer.size()))
                    .getKey();
              };

      var start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < maxTime) {
        possibleAssignments.add(
            tmpCost.keySet().stream()
                .map(tp -> Map.entry(randomAssign.apply(tp), tp))
                .collect(
                    Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList()))));
      }

      var incompatibility =
          (Function<Map<String, List<TopicPartition>>, Long>)
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
                    .mapToLong(
                        e ->
                            e.getValue().stream()
                                    .filter(tp -> unsuit.get(e.getKey()).contains(tp))
                                    .count()
                                / 2)
                    .sum();
              };

      var sigma =
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
      var w =
          possibleAssignments.stream()
              .map(r -> Map.entry(incompatibility.apply(r), r))
              .collect(
                  Collectors.groupingBy(
                      Map.Entry::getKey,
                      Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));
      System.out.println("w = " + w);
      var resu =
          w.entrySet().stream().min(Map.Entry.comparingByKey()).get().getValue().stream()
              .min(Comparator.comparingDouble(sigma::apply))
              .get();
      System.out.println("resu = " + resu);
      return resu;
    };
  }
}
