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
package org.astraea.app.cost.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;

public class GetPartitionInf {
  static Map<Integer, Map<TopicPartition, Integer>> getSize(Admin client) {
    Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize = new HashMap<>();
    client
        .brokerIds()
        .forEach(
            (broker) -> {
              var partitionSize = new TreeMap<TopicPartition, Integer>();
              client
                  .replicas(client.topicNames())
                  .forEach(
                      (tp, assignment) -> {
                        assignment.forEach(
                            partition -> {
                              if (partition.broker() == broker)
                                partitionSize.put(tp, (int) partition.size());
                            });
                        brokerPartitionSize.put(broker, partitionSize);
                      });
            });
    return brokerPartitionSize;
  }

  static Map<String, Integer> getRetentionMillis(Admin client) {
    return client.topics().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry
                        .getValue()
                        .value("retention.ms")
                        .map(Integer::parseInt)
                        .orElseThrow(
                            () -> new NoSuchElementException("retention.ms does not exist"))));
  }
}
