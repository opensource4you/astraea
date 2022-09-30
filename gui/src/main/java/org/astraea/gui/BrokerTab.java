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
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.TopicPartition;

public class BrokerTab {

  public static Tab of(Context context) {
    var pane =
        Utils.searchToTable(
            "search for brokers:",
            word ->
                context
                    .optionalAdmin()
                    .map(
                        admin ->
                            admin.nodes().stream()
                                .filter(
                                    nodeInfo ->
                                        word.isEmpty()
                                            || String.valueOf(nodeInfo.id()).contains(word)
                                            || nodeInfo.host().contains(word)
                                            || String.valueOf(nodeInfo.port()).contains(word))
                                .map(
                                    node ->
                                        LinkedHashMap.of(
                                            "hostname", node.host(),
                                            "id", String.valueOf(node.id()),
                                            "port", String.valueOf(node.port()),
                                            "topics",
                                                String.valueOf(
                                                    node.folders().stream()
                                                        .flatMap(
                                                            d ->
                                                                d.partitionSizes().keySet().stream()
                                                                    .map(TopicPartition::topic))
                                                        .distinct()
                                                        .count()),
                                            "partitions",
                                                String.valueOf(
                                                    node.folders().stream()
                                                        .flatMap(
                                                            d ->
                                                                d
                                                                    .partitionSizes()
                                                                    .keySet()
                                                                    .stream())
                                                        .distinct()
                                                        .count()),
                                            "size",
                                                DataSize.Byte.of(
                                                        node.folders().stream()
                                                            .mapToLong(
                                                                d ->
                                                                    d
                                                                        .partitionSizes()
                                                                        .values()
                                                                        .stream()
                                                                        .mapToLong(v -> v)
                                                                        .sum())
                                                            .sum())
                                                    .toString()))
                                .collect(Collectors.toList()))
                    .orElse(List.of()));
    var tab = new Tab("node");
    tab.setContent(pane);
    return tab;
  }
}
