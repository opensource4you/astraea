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
import org.astraea.common.Configuration;
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Shuffler {
  Map<String, List<TopicPartition>> shuffle();

  static Shuffler randomShuffler(
      Map<String, SubscriptionInfo> subscriptions,
      Map<TopicPartition, Double> partitionCost,
      Map<TopicPartition, Set<TopicPartition>> incompatible,
      Configuration config) {
    var timeLimiter = Limiter.timeLimiter(config);
    var limiters =
        Limiter.of(
            Set.of(
                Limiter.skewCostLimiter(partitionCost, subscriptions),
                Limiter.incompatibleLimiter(incompatible)));
    var hints =
        Hint.of(
            Set.of(
                Hint.lowCostHint(subscriptions, partitionCost),
                Hint.incompatibleHint(subscriptions, incompatible)));

    return () -> {
      var generator =
          Generator.randomGenerator(subscriptions.keySet(), partitionCost.keySet(), hints);
      Map<String, List<TopicPartition>> result = null;
      var possibleCombinations = new PossibleCombination(partitionCost, incompatible);
      var start = System.currentTimeMillis();

      while (timeLimiter.check(start)) {
        var combinator = generator.get();
        possibleCombinations.add(combinator);
        if (limiters.check(combinator)) {
          result = combinator;
          break;
        }
      }
      return result == null ? possibleCombinations.get() : result;
    };
  }
}
