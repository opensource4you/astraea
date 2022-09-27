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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;

public class ConsumerTab {

  static final Map<String, Function<Bean, Object>> COLUMN_AND_BEAN =
      LinkedHashMap.of(
          "group",
          bean -> bean.groupId,
          "topic",
          bean -> bean.topic,
          "partition",
          bean -> bean.partition,
          "offset",
          bean -> bean.offset,
          "member id",
          bean -> bean.memberId,
          "host",
          bean -> bean.host,
          "client id",
          bean -> bean.clientId,
          "instance id",
          bean -> bean.groupInstanceId.orElse(""));

  public static Tab of(Context context) {
    var pane =
        context.tableView(
            (admin, word) ->
                Context.result(
                    COLUMN_AND_BEAN,
                    admin
                        .consumerGroups(
                            admin.consumerGroupIds().stream()
                                .filter(group -> word.isEmpty() || group.contains(word))
                                .collect(Collectors.toSet()))
                        .stream()
                        .flatMap(
                            v ->
                                v.assignment().entrySet().stream()
                                    .flatMap(
                                        entry ->
                                            entry.getValue().stream()
                                                .map(
                                                    tp ->
                                                        new Bean(
                                                            v.groupId(),
                                                            tp.topic(),
                                                            tp.partition(),
                                                            v.consumeProgress()
                                                                .getOrDefault(tp, -1L),
                                                            entry.getKey().memberId(),
                                                            entry.getKey().groupInstanceId(),
                                                            entry.getKey().clientId(),
                                                            entry.getKey().host()))))
                        .collect(Collectors.toList())));
    var tab = new Tab("consumer");
    tab.setContent(pane);
    return tab;
  }

  public static class Bean {
    private final String groupId;
    private final String topic;
    private final int partition;
    private final long offset;
    private final String memberId;
    private final Optional<String> groupInstanceId;
    private final String clientId;
    private final String host;

    public Bean(
        String groupId,
        String topic,
        int partition,
        long offset,
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host) {
      this.groupId = groupId;
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
      this.memberId = memberId;
      this.groupInstanceId = groupInstanceId;
      this.clientId = clientId;
      this.host = host;
    }
  }
}
