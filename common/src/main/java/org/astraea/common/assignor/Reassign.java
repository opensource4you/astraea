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
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Reassign {
  /**
   * Try to avoid putting incompatible partitions on the same consumer.
   *
   * @param subscriptions the subscription of consumers
   * @param assignment assignment
   * @param incompatible incompatible partition calculated by cost function
   * @param costs partition cost
   * @return assignment that filter out most incompatible partitions
   */
  Map<String, List<TopicPartition>> result(
      Map<String, SubscriptionInfo> subscriptions,
      Map<String, List<TopicPartition>> assignment,
      Map<TopicPartition, Set<TopicPartition>> incompatible,
      Map<TopicPartition, Double> costs);

  static Reassign incompatible() {
    return (subscriptions, assignment, incompatible, costs) -> {
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

      // filter incompatible partitions from assignment to get remaining assignment
      var remaining =
          assignment.keySet().stream()
              .map(
                  consumer ->
                      Map.entry(
                          consumer,
                          assignment.get(consumer).stream()
                              .filter(tp -> !unsuitable.get(consumer).contains(tp))
                              .collect(Collectors.toList())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // calculate remaining cost for further assign
      var remainingCost =
          remaining.entrySet().stream()
              .map(e -> Map.entry(e.getKey(), e.getValue().stream().mapToDouble(costs::get).sum()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // the partitions have been assigned
      var assigned =
          remaining.values().stream().flatMap(List::stream).collect(Collectors.toUnmodifiableSet());
      // the partitions need to be reassigned
      var unassigned =
          assignment.values().stream()
              .flatMap(Collection::stream)
              .filter(tp -> !assigned.contains(tp))
              .collect(Collectors.toSet());

      if (unassigned.isEmpty()) return assignment;

      String minConsumer;
      for (var tp : unassigned) {
        // find the consumers that subscribe the topic which we assign now
        var subscribedConsumer =
            subscriptions.entrySet().stream()
                .filter(e -> e.getValue().topics().contains(tp.topic()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        // find the consumers that are suitable with the tp
        var suitableConsumer =
            remaining.entrySet().stream()
                .filter(e -> subscribedConsumer.contains(e.getKey()))
                .map(
                    e ->
                        Map.entry(
                            e.getKey(),
                            e.getValue().stream()
                                .flatMap(p -> incompatible.get(p).stream())
                                .collect(Collectors.toSet())))
                .filter(e -> !e.getValue().contains(tp))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // if there is no suitable consumer, choose the lowest cost consumer that subscribed topic
        // and assign the tp to it.
        // Otherwise, choose the lowest cost consumer from suitable consumers and assign the tp to
        // it
        minConsumer =
            suitableConsumer.isEmpty()
                ? remainingCost.entrySet().stream()
                    .filter(e -> subscribedConsumer.contains(e.getKey()))
                    .min(Map.Entry.comparingByValue())
                    .get()
                    .getKey()
                : remainingCost.entrySet().stream()
                    .filter(e -> suitableConsumer.contains(e.getKey()))
                    .min(Map.Entry.comparingByValue())
                    .get()
                    .getKey();

        remaining.get(minConsumer).add(tp);
        remainingCost.compute(minConsumer, (ignore, totalCost) -> totalCost + costs.get(tp));
      }

      return remaining;
    };
  }
}
