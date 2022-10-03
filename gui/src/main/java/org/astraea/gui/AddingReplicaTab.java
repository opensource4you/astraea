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
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;

public class AddingReplicaTab {

  static final Map<String, Function<Bean, Object>> COLUMN_AND_BEAN =
      LinkedHashMap.of(
          "topic",
          bean -> bean.topic,
          "partition",
          bean -> bean.partition,
          "broker",
          bean -> bean.broker,
          "path",
          bean -> bean.path,
          "size",
          bean -> bean.size,
          "leader size",
          bean -> bean.leaderSize,
          "progress",
          bean -> bean.progress);

  public static Tab of(Context context) {
    var tab = new Tab("adding replica");
    tab.setContent(
        Utils.searchToTable(
            "topic name or broker id (space means all topics/brokers):",
            (word, console) ->
                context
                    .optionalAdmin()
                    .map(
                        admin ->
                            admin.addingReplicas(admin.topicNames()).stream()
                                .filter(
                                    s ->
                                        word.isEmpty()
                                            || s.topic().contains(word)
                                            || String.valueOf(s.broker()).contains(word))
                                .map(
                                    state ->
                                        LinkedHashMap.of(
                                            "topic",
                                            state.topic(),
                                            "partition",
                                            String.valueOf(state.partition()),
                                            "broker",
                                            String.valueOf(state.broker()),
                                            "path",
                                            state.path(),
                                            "size",
                                            String.valueOf(state.size()),
                                            "leader size",
                                            String.valueOf(state.leaderSize()),
                                            "progress",
                                            String.format(
                                                "%.2f%%",
                                                state.leaderSize() == 0
                                                    ? 100D
                                                    : ((double) state.size()
                                                            / (double) state.leaderSize())
                                                        * 100)))
                                .collect(Collectors.toList()))
                    .orElse(List.of())));
    return tab;
  }

  public static class Bean {
    private final String topic;
    private final int partition;
    private final int broker;
    private final String path;
    private final long size;
    private final long leaderSize;

    private final String progress;

    public Bean(String topic, int partition, int broker, String path, long size, long leaderSize) {
      this.topic = topic;
      this.partition = partition;
      this.broker = broker;
      this.path = path;
      this.size = size;
      this.leaderSize = leaderSize;
      this.progress =
          String.format(
              "%.2f%%", leaderSize == 0 ? 100D : ((double) size / (double) leaderSize) * 100);
    }
  }
}
