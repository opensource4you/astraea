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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;

public class PartitionTab {

  static final Map<String, Function<Bean, Object>> COLUMN_AND_BEAN =
      LinkedHashMap.of(
          "topic",
          bean -> bean.topic,
          "partition",
          bean -> bean.partition,
          "leader",
          bean -> bean.leader,
          "replicas",
          bean -> bean.replicas.stream().map(String::valueOf).collect(Collectors.joining(",")),
          "isr",
          bean -> bean.isr.stream().map(String::valueOf).collect(Collectors.joining(",")),
          "earliest offset",
          bean -> bean.earliestOffset,
          "latest offset",
          bean -> bean.latestOffset,
          "max timestamp",
          bean -> bean.maxTimestamp);

  public static Tab of(Context context) {
    var pane =
        context.tableView(
            "search for topics:",
            (admin, word) ->
                Context.result(
                    COLUMN_AND_BEAN,
                    admin
                        .partitions(
                            admin.topicNames().stream()
                                .filter(name -> word.isEmpty() || name.contains(word))
                                .collect(Collectors.toSet()))
                        .stream()
                        .sorted(
                            Comparator.comparing(Partition::topic)
                                .thenComparing(Partition::partition))
                        .map(Bean::new)
                        .collect(Collectors.toList())));
    var tab = new Tab("partition");
    tab.setContent(pane);
    return tab;
  }

  public static class Bean {
    private final String topic;
    private final int partition;

    private final int leader;
    private final List<Integer> replicas;
    private final List<Integer> isr;

    private final long earliestOffset;

    private final long latestOffset;

    private final String maxTimestamp;

    public Bean(Partition partition) {
      this.topic = partition.topic();
      this.partition = partition.partition();
      this.leader = partition.leader().id();
      this.replicas = partition.replicas().stream().map(NodeInfo::id).collect(Collectors.toList());
      this.isr = partition.isr().stream().map(NodeInfo::id).collect(Collectors.toList());
      this.earliestOffset = partition.earliestOffset();
      this.latestOffset = partition.latestOffset();
      if (partition.maxTimestamp() > 0) {
        var format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        maxTimestamp = format.format(new Date(partition.maxTimestamp()));
      } else maxTimestamp = "unknown";
    }
  }
}
