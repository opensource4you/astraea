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
package org.astraea.gui.tab;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.pane.TabPane;

public class ReplicaTab {

  private static List<Map<String, Object>> offlineResult(List<Replica> replicas) {
    return replicas.stream()
        .filter(ReplicaInfo::isOffline)
        .map(
            replica ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    replica.topic(),
                    "partition",
                    replica.partition(),
                    "broker",
                    replica.nodeInfo().id(),
                    "leader",
                    replica.isLeader(),
                    "isPreferredLeader",
                    replica.isPreferredLeader()))
        .collect(Collectors.toList());
  }

  private static List<Map<String, Object>> syncingResult(List<Replica> replicas) {
    var leaderSizes =
        replicas.stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toMap(ReplicaInfo::topicPartition, Replica::size));
    return replicas.stream()
        .filter(r -> !r.inSync())
        .filter(r -> r.path() != null)
        .filter(ReplicaInfo::isOnline)
        .map(
            replica -> {
              var leaderSize = leaderSizes.getOrDefault(replica.topicPartition(), 0L);
              return LinkedHashMap.<String, Object>of(
                  "topic",
                  replica.topic(),
                  "partition",
                  replica.partition(),
                  "broker",
                  replica.nodeInfo().id(),
                  "path",
                  replica.path(),
                  "leader",
                  replica.isLeader(),
                  "isPreferredLeader",
                  replica.isPreferredLeader(),
                  "size",
                  DataSize.Byte.of(replica.size()),
                  "leader size",
                  DataSize.Byte.of(leaderSize),
                  "progress",
                  String.format(
                      "%.2f%%",
                      leaderSize == 0
                          ? 100D
                          : ((double) replica.size() / (double) leaderSize) * 100));
            })
        .collect(Collectors.toList());
  }

  private static List<Map<String, Object>> allResult(List<Replica> replicas) {
    return replicas.stream()
        .map(
            replica ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    replica.topic(),
                    "partition",
                    replica.partition(),
                    "broker",
                    replica.nodeInfo().id(),
                    "path",
                    replica.path() == null ? "unknown" : replica.path(),
                    "leader",
                    replica.isLeader(),
                    "isPreferredLeader",
                    replica.isPreferredLeader(),
                    "offline",
                    replica.isOffline(),
                    "future",
                    replica.isFuture(),
                    "lag",
                    replica.lag(),
                    "size",
                    DataSize.Byte.of(replica.size())))
        .collect(Collectors.toList());
  }

  public static Tab basicTab(Context context) {
    var all = "all";
    var syncing = "syncing";
    var offline = "offline";
    return Tab.of(
        "basic",
        PaneBuilder.of()
            .radioButtons(List.of(all, syncing, offline))
            .searchField("topic name")
            .buttonAction(
                (input, logger) ->
                    context
                        .admin()
                        .topicNames(true)
                        .thenApply(
                            topics ->
                                topics.stream()
                                    .filter(input::matchSearch)
                                    .collect(Collectors.toSet()))
                        .thenCompose(context.admin()::replicas)
                        .thenApply(
                            replicas -> {
                              var selected = input.selectedRadio().map(s -> (String) s).orElse(all);
                              if (selected.equals(syncing)) return syncingResult(replicas);
                              if (selected.equals(offline)) return offlineResult(replicas);
                              return allResult(replicas);
                            }))
            .build());
  }

  public static Tab of(Context context) {
    return Tab.of("replica", TabPane.of(Side.TOP, List.of(basicTab(context))));
  }
}
