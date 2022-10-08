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
package org.astraea.gui;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.AddingReplica;

public class AddingReplicaTab {

  private static List<Map<String, Object>> result(Stream<AddingReplica> replicas) {
    return replicas
        .map(
            state ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    state.topic(),
                    "partition",
                    state.partition(),
                    "broker",
                    state.broker(),
                    "path",
                    state.path(),
                    "size",
                    state.size(),
                    "leader size",
                    state.leaderSize(),
                    "progress",
                    String.format(
                        "%.2f%%",
                        state.leaderSize() == 0
                            ? 100D
                            : ((double) state.size() / (double) state.leaderSize()) * 100)))
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .searchField("topic name")
            .outputTable(
                input ->
                    context.submit(
                        admin ->
                            admin
                                .topicNames(true)
                                .thenCompose(admin::addingReplicas)
                                .thenApply(
                                    rs ->
                                        rs.stream()
                                            .filter(
                                                s ->
                                                    input.matchSearch(s.topic())
                                                        || input.matchSearch(
                                                            String.valueOf(s.broker()))))
                                .thenApply(AddingReplicaTab::result)))
            .build();

    var tab = new Tab("adding replica");
    tab.setContent(pane);
    return tab;
  }
}
