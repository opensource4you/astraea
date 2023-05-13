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
import org.astraea.common.admin.TopicPartition;

@FunctionalInterface
public interface Hint {
  List<String> get(TopicPartition tp);

  static Hint lowCostHint(
      Map<String, SubscriptionInfo> subscriptions, Map<String, Double> consumerCost) {
    return (tp) -> {
      var consumers =
          subscriptions.entrySet().stream()
              .filter(e -> e.getValue().topics().contains(tp.topic()))
              .map(Map.Entry::getKey)
              .toList();

      return consumerCost.entrySet().stream()
          .filter(e -> consumers.contains(e.getKey()))
          .sorted(Map.Entry.comparingByValue())
          .limit((long) Math.ceil(consumerCost.size() / 2.0))
          .map(Map.Entry::getKey)
          .toList();
    };
  }
}
