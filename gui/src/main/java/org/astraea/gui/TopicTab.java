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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Node;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;

public class TopicTab {

  static final Map<String, Function<Bean, Object>> COLUMN_AND_BEAN =
      LinkedHashMap.of(
          "name",
          bean -> bean.name,
          "partitions",
          bean -> bean.partitions,
          "replicas",
          bean -> bean.replicas,
          "size",
          bean -> bean.size,
          "brokers",
          bean -> bean.brokerIds.stream().map(String::valueOf).collect(Collectors.joining(",")),
          "max timestamp",
          bean -> bean.maxTimestamp);

  public static Tab of(Context context) {
    var pane =
        context.tableView(
            (admin, word) ->
                Context.result(
                    COLUMN_AND_BEAN,
                    beans(
                        admin.partitions(
                            admin.topicNames().stream()
                                .filter(name -> word.isEmpty() || name.contains(word))
                                .collect(Collectors.toSet())),
                        admin.nodes())));
    var tab = new Tab("topic");
    tab.setContent(pane);
    return tab;
  }

  private static List<Bean> beans(List<Partition> partitions, List<Node> nodes) {
    var topicSize =
        nodes.stream()
            .flatMap(n -> n.folders().stream().flatMap(d -> d.partitionSizes().entrySet().stream()))
            .collect(Collectors.groupingBy(e -> e.getKey().topic()));
    return partitions.stream()
        .map(Partition::topic)
        .distinct()
        .sorted()
        .map(
            topic ->
                new Bean(
                    topic,
                    (int) partitions.stream().filter(tp -> tp.topic().equals(topic)).count(),
                    (int)
                        topicSize.values().stream()
                            .mapToLong(
                                entries ->
                                    entries.stream()
                                        .filter(tp -> tp.getKey().topic().equals(topic))
                                        .count())
                            .sum(),
                    DataSize.Byte.of(
                        topicSize.get(topic).stream().mapToLong(Map.Entry::getValue).sum()),
                    partitions.stream()
                        .filter(p -> p.topic().equals(topic))
                        .flatMap(p -> p.replicas().stream().map(NodeInfo::id))
                        .collect(Collectors.toSet()),
                    partitions.stream()
                        .filter(tp -> tp.topic().equals(topic))
                        .mapToLong(Partition::maxTimestamp)
                        .max()
                        .orElse(-1)))
        .collect(Collectors.toList());
  }

  public static class Bean {
    private final String name;
    private final int partitions;
    private final int replicas;
    private final DataSize size;
    private final Set<Integer> brokerIds;

    private final String maxTimestamp;

    public Bean(
        String name,
        int partitions,
        int replicas,
        DataSize size,
        Set<Integer> brokerIds,
        long maxTimestamp) {
      this.name = name;
      this.partitions = partitions;
      this.replicas = replicas;
      this.size = size;
      this.brokerIds = brokerIds;
      if (maxTimestamp > 0) {
        var format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        this.maxTimestamp = format.format(new Date(maxTimestamp));
      } else this.maxTimestamp = "unknown";
    }
  }
}
