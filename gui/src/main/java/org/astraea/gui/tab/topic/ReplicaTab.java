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
package org.astraea.gui.tab.topic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class ReplicaTab {

  private static List<Map<String, Object>> allResult(List<Replica> replicas) {
    var leaderSizes =
        replicas.stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toMap(ReplicaInfo::topicPartition, Replica::size));
    return replicas.stream()
        .map(
            replica -> {
              var leaderSize = leaderSizes.getOrDefault(replica.topicPartition(), 0L);
              var result = new LinkedHashMap<String, Object>();
              result.put("topic", replica.topic());
              result.put("partition", replica.partition());
              result.put("broker", replica.nodeInfo().id());
              if (replica.path() != null) result.put("path", replica.path());
              result.put("isLeader", replica.isLeader());
              result.put("isPreferredLeader", replica.isPreferredLeader());
              result.put("isOffline", replica.isOffline());
              result.put("isFuture", replica.isFuture());
              result.put("lag", replica.lag());
              result.put("size", DataSize.Byte.of(replica.size()));
              if (leaderSize > replica.size()) {
                result.put("leader size", DataSize.Byte.of(leaderSize));
                result.put(
                    "progress",
                    String.format(
                        "%.2f%%",
                        leaderSize == 0
                            ? 100D
                            : ((double) replica.size() / (double) leaderSize) * 100));
              }
              return result;
            })
        .collect(Collectors.toList());
  }

  static Tab tab(Context context) {
    return Tab.of(
        "replica",
        PaneBuilder.of()
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenCompose(context.admin()::replicas)
                        .thenApply(ReplicaTab::allResult))
            .build());
  }
}
