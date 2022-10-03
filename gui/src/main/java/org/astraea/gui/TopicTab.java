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
import java.util.Optional;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;

public class TopicTab {
  public static Tab of(Context context) {

    var pane =
        Utils.searchToTable(
            (word, console) ->
                context.submit(
                    admin ->
                        beans(
                            admin.partitions(
                                admin.topicNames().stream()
                                    .filter(name -> word.isEmpty() || name.contains(word))
                                    .collect(Collectors.toSet())),
                            admin.brokers())));
    var tab = new Tab("topic");
    tab.setContent(pane);
    return tab;
  }

  private static List<Map<String, Object>> beans(List<Partition> partitions, List<Broker> nodes) {
    var topicSize =
        nodes.stream()
            .flatMap(n -> n.folders().stream().flatMap(d -> d.partitionSizes().entrySet().stream()))
            .collect(Collectors.groupingBy(e -> e.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Map.Entry::getValue).sum()));
    var tps = partitions.stream().collect(Collectors.groupingBy(Partition::topic));
    return tps.keySet().stream()
        .map(
            topic ->
                LinkedHashMap.<String, Object>of(
                    "name", topic,
                    "partitions", tps.get(topic).size(),
                    "replicas", tps.get(topic).stream().mapToInt(p -> p.replicas().size()).sum(),
                    "size",
                        Optional.ofNullable(topicSize.get(topic))
                            .map(s -> DataSize.Byte.of(s).toString())
                            .orElse("unknown"),
                    "broker ids",
                        tps.get(topic).stream()
                            .flatMap(p -> p.replicas().stream().map(NodeInfo::id))
                            .map(String::valueOf)
                            .distinct()
                            .sorted()
                            .collect(Collectors.joining(",")),
                    "max timestamp",
                        Utils.format(
                            tps.get(topic).stream()
                                .mapToLong(Partition::maxTimestamp)
                                .max()
                                .orElse(-1L))))
        .collect(Collectors.toList());
  }
}
