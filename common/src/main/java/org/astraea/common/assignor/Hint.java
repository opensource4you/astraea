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
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Hint {
  List<String> get(Map<String, List<TopicPartition>> currentAssignment, TopicPartition tp);

  static Hint of(Set<Hint> hints) {
    return (currentAssignment, tp) ->
        hints.stream()
            .map(h -> h.get(currentAssignment, tp))
            .reduce((l1, l2) -> l1.stream().filter(l2::contains).toList())
            .get();
  }

  static Hint lowCostHint(
      Map<String, SubscriptionInfo> subscriptions, Map<TopicPartition, Double> partitionCost) {
    return (currentAssignment, tp) -> {
      var candidates =
          currentAssignment.entrySet().stream()
              .filter(e -> subscriptions.get(e.getKey()).topics().contains(tp.topic()))
              .map(
                  e ->
                      Map.entry(
                          e.getKey(), e.getValue().stream().mapToDouble(partitionCost::get).sum()))
              .sorted(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey)
              .toList();

      return candidates.stream().limit((long) Math.ceil(candidates.size() / 2.0)).toList();
    };
  }

  static Hint incompatibleHint(
      Map<String, SubscriptionInfo> subscriptions,
      Map<TopicPartition, Set<TopicPartition>> incompatibilities) {
    return (currentAssignment, tp) -> {
      var subscriber =
          subscriptions.entrySet().stream()
              .filter(e -> e.getValue().topics().contains(tp.topic()))
              .map(Map.Entry::getKey)
              .toList();
      if (incompatibilities.containsKey(tp) && incompatibilities.get(tp).isEmpty())
        return subscriber;

      var candidates =
          currentAssignment.entrySet().stream()
              .filter(e -> subscriber.contains(e.getKey()))
              .map(
                  e ->
                      Map.entry(
                          e.getKey(),
                          e.getValue().stream()
                              .filter(incompatibilities::containsKey)
                              .filter(p -> incompatibilities.get(p).contains(tp))
                              .count()))
              .collect(
                  Collectors.groupingBy(
                      Map.Entry::getValue,
                      Collectors.mapping(Map.Entry::getKey, Collectors.toList())))
              .entrySet()
              .stream()
              .min(Map.Entry.comparingByKey())
              .get()
              .getValue();

      return candidates.isEmpty() ? List.of() : candidates;
    };
  }
}
