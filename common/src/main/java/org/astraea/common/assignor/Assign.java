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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Assign {
  Map<String, List<TopicPartition>> strategy(
      Map<String, SubscriptionInfo> subscriptions, Map<TopicPartition, Double> costs);

    /**
     * implement the greedy assign strategy
     * @return the assignment by greedy strategy
     */
  static Assign greedy() {
    return (subscriptions, costs) -> {
      var tmpConsumerCost =
          subscriptions.keySet().stream()
              .collect(Collectors.toMap(Function.identity(), ignore -> 0.0D));

      var lowestCostConsumer =
          (Function<TopicPartition, String>)
              (tp) ->
                  tmpConsumerCost.entrySet().stream()
                      .filter(e -> subscriptions.get(e.getKey()).topics().contains(tp.topic()))
                      .min(Map.Entry.comparingByValue())
                      .get()
                      .getKey();

      return costs.entrySet().stream()
          .map(
              e -> {
                var consumer = lowestCostConsumer.apply(e.getKey());
                tmpConsumerCost.compute(consumer, (ignore, totalCost) -> totalCost + e.getValue());
                return Map.entry(consumer, e.getKey());
              })
          .collect(
              Collectors.groupingBy(
                  Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
    };
  }
}
