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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;

public class RandomAssignor extends Assignor {

  @Override
  public Map<String, List<TopicPartition>> assign(
      Map<String, org.astraea.common.assignor.Subscription> subscriptions,
      ClusterInfo clusterInfo) {
    var assignments = new HashMap<String, List<TopicPartition>>();
    var consumers = new ArrayList<>(subscriptions.keySet());
    Set<String> topics = new HashSet<>();
    consumers.forEach(consumer -> assignments.put(consumer, new ArrayList<>()));

    for (org.astraea.common.assignor.Subscription subscription : subscriptions.values())
      topics.addAll(subscription.topics());

    clusterInfo.topicPartitions().stream()
        .filter(tp -> topics.contains(tp.topic()))
        .forEach(
            tp -> {
              var consumer = consumers.get((int) (Math.random() * consumers.size()));
              assignments.get(consumer).add(tp);
            });

    return assignments;
  }

  @Override
  public String name() {
    return "random";
  }
}
