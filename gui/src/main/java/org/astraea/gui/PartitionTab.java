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
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Partition;

public class PartitionTab {
  public static Tab of(Context context) {

    var pane =
        Utils.searchToTable(
            "search for topics:",
            word ->
                context
                    .optionalAdmin()
                    .map(
                        admin ->
                            admin
                                .partitions(
                                    admin.topicNames().stream()
                                        .filter(name -> word.isEmpty() || name.contains(word))
                                        .collect(Collectors.toSet()))
                                .stream()
                                .sorted(
                                    Comparator.comparing(Partition::topic)
                                        .thenComparing(Partition::partition))
                                .map(
                                    p ->
                                        LinkedHashMap.of(
                                            "topic",
                                            p.topic(),
                                            "partition",
                                            String.valueOf(p.partition()),
                                            "leader",
                                            String.valueOf(p.leader().id()),
                                            "replicas",
                                            p.replicas().stream()
                                                .map(n -> String.valueOf(n.id()))
                                                .collect(Collectors.joining(",")),
                                            "isr",
                                            p.isr().stream()
                                                .map(n -> String.valueOf(n.id()))
                                                .collect(Collectors.joining(",")),
                                            "earliest offset",
                                            String.valueOf(p.earliestOffset()),
                                            "latest offset",
                                            String.valueOf(p.latestOffset()),
                                            "max timestamp",
                                            String.valueOf(p.maxTimestamp())))
                                .collect(Collectors.toList()))
                    .orElse(List.of()));
    var tab = new Tab("partition");
    tab.setContent(pane);
    return tab;
  }
}
