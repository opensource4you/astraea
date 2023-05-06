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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Generator {
  PossibleAssignments produce(Duration time);

  /**
   * Randomly generate assignments and use the total cost of the consumers to guide the distribution
   * of the random assignments towards consumers that are more likely to find them useful, thereby
   * reducing the number of useless assignments.
   *
   * @param subscription the subscription of consumer group
   * @param costs partitions' cost
   * @return Generator of random assignments
   */
  static Generator random(
      Map<String, SubscriptionInfo> subscription, Map<TopicPartition, Double> costs) {
    return (t) -> {
      var result = new HashSet<Map<String, List<TopicPartition>>>();
      var tmpCost =
          subscription.keySet().stream()
              .map(c -> Map.entry(c, 0.0))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      var randomPick =
          (BiFunction<Map<String, Double>, TopicPartition, String>)
              (hint, tp) -> {
                var possibles =
                    subscription.entrySet().stream()
                        .filter(e -> e.getValue().topics().contains(tp.topic()))
                        .map(e -> Map.entry(e.getKey(), hint.get(e.getKey())))
                        .sorted(Map.Entry.comparingByValue())
                        .toList();
                var candidates =
                    possibles.stream().limit((long) Math.ceil(possibles.size() / 2.0)).toList();

                return candidates
                    .get(ThreadLocalRandom.current().nextInt(candidates.size()))
                    .getKey();
              };
      var start = System.currentTimeMillis();

      while (System.currentTimeMillis() - start < t.toMillis()) {
        result.add(
            costs.keySet().stream()
                .map(tp -> Map.entry(randomPick.apply(tmpCost, tp), tp))
                .collect(
                    Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList()))));
        tmpCost.forEach((k, v) -> tmpCost.replace(k, 0.0));
      }

      return new PossibleAssignments(result);
    };
  }
}
