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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;

public class PartitionTab {

  private static List<Map<String, Object>> result(Stream<Partition> ps) {
    return ps.map(
            p ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    p.topic(),
                    "partition",
                    p.partition(),
                    "leader",
                    p.leader().map(NodeInfo::id).orElse(-1),
                    "replicas",
                    p.replicas().stream()
                        .map(n -> String.valueOf(n.id()))
                        .collect(Collectors.joining(",")),
                    "isr",
                    p.isr().stream()
                        .map(n -> String.valueOf(n.id()))
                        .collect(Collectors.joining(",")),
                    "earliest offset",
                    p.earliestOffset(),
                    "latest offset",
                    p.latestOffset(),
                    "max timestamp",
                    Utils.format(p.maxTimestamp())))
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
                                .thenApply(
                                    names ->
                                        names.stream()
                                            .filter(input::matchSearch)
                                            .collect(Collectors.toSet()))
                                .thenCompose(admin::partitions)
                                .thenApply(
                                    ps ->
                                        ps.stream()
                                            .sorted(
                                                Comparator.comparing(Partition::topic)
                                                    .thenComparing(Partition::partition)))
                                .thenApply(PartitionTab::result)))
            .build();
    var tab = new Tab("partition");
    tab.setContent(pane);
    return tab;
  }
}
