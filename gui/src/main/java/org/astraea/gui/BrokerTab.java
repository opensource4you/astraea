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
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.TopicPartition;

public class BrokerTab {

  private static List<Map<String, Object>> result(Stream<Broker> brokers) {
    return brokers
        .map(
            broker ->
                LinkedHashMap.<String, Object>of(
                    "hostname",
                    broker.host(),
                    "broker id",
                    broker.id(),
                    "port",
                    broker.port(),
                    "controller",
                    broker.isController(),
                    "topics",
                    broker.folders().stream()
                        .flatMap(
                            d -> d.partitionSizes().keySet().stream().map(TopicPartition::topic))
                        .distinct()
                        .count(),
                    "partitions",
                    broker.folders().stream()
                        .flatMap(d -> d.partitionSizes().keySet().stream())
                        .distinct()
                        .count(),
                    "leaders",
                    broker.topicPartitionLeaders().size(),
                    "size",
                    DataSize.Byte.of(
                        broker.folders().stream()
                            .mapToLong(
                                d -> d.partitionSizes().values().stream().mapToLong(v -> v).sum())
                            .sum())))
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .searchField("broker id/host")
            .buttonTableAction(
                input ->
                    context.submit(
                        admin ->
                            admin
                                .brokers()
                                .thenApply(
                                    brokers ->
                                        brokers.stream()
                                            .filter(
                                                nodeInfo ->
                                                    input.matchSearch(String.valueOf(nodeInfo.id()))
                                                        || input.matchSearch(nodeInfo.host())))
                                .thenApply(BrokerTab::result)))
            .build();
    var tab = new Tab("broker");
    tab.setContent(pane);
    return tab;
  }
}
