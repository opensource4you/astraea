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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Node;
import org.astraea.common.admin.TopicPartition;

public class NodeTab {

  static final Map<String, Function<Bean, Object>> COLUMN_AND_BEAN =
      LinkedHashMap.of(
          "hostname",
          bean -> bean.host,
          "id",
          bean -> bean.id,
          "port",
          bean -> bean.port,
          "topics",
          bean -> bean.topics,
          "partitions",
          bean -> bean.partitions,
          "size",
          bean -> bean.size);

  public static Tab of(Context context) {
    var pane =
        context.tableView(
            "search for nodes:",
            (admin, word) ->
                Context.result(
                    COLUMN_AND_BEAN,
                    admin.nodes().stream()
                        .filter(
                            nodeInfo ->
                                word.isEmpty()
                                    || String.valueOf(nodeInfo.id()).contains(word)
                                    || nodeInfo.host().contains(word)
                                    || String.valueOf(nodeInfo.port()).contains(word))
                        .map(Bean::new)
                        .collect(Collectors.toList())));
    var tab = new Tab("node");
    tab.setContent(pane);
    return tab;
  }

  public static class Bean {
    private final String host;
    private final int id;
    private final int port;

    private final int topics;
    private final int partitions;

    private final DataSize size;

    private Bean(Node node) {
      this.host = node.host();
      this.id = node.id();
      this.port = node.port();
      this.topics =
          (int)
              node.folders().stream()
                  .flatMap(d -> d.partitionSizes().keySet().stream().map(TopicPartition::topic))
                  .distinct()
                  .count();
      this.partitions =
          (int)
              node.folders().stream()
                  .flatMap(d -> d.partitionSizes().keySet().stream())
                  .distinct()
                  .count();
      this.size =
          DataSize.Byte.of(
              node.folders().stream()
                  .mapToLong(d -> d.partitionSizes().values().stream().mapToLong(v -> v).sum())
                  .sum());
    }
  }
}
