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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.ProducerState;

public class ProducerTab {

  static final Map<String, Function<Bean, Object>> COLUMN_AND_BEAN =
      LinkedHashMap.of(
          "topic",
          bean -> bean.topic,
          "partition",
          bean -> bean.partition,
          "producer id",
          bean -> bean.producerId,
          "producer epoch",
          bean -> bean.producerEpoch,
          "last sequence",
          bean -> bean.lastSequence,
          "last timestamp",
          bean -> bean.lastTimestamp);

  public static Tab of(Context context) {
    var pane =
        context.tableView(
            "search for topics:",
            (admin, word) ->
                Context.result(
                    COLUMN_AND_BEAN,
                    admin
                        .producerStates(
                            admin.topicPartitions(
                                admin.topicNames().stream()
                                    .filter(name -> word.isEmpty() || name.contains(word))
                                    .collect(Collectors.toSet())))
                        .stream()
                        .sorted(
                            Comparator.comparing(ProducerState::topic)
                                .thenComparing(ProducerState::partition))
                        .map(
                            state ->
                                new Bean(
                                    state.topic(),
                                    state.partition(),
                                    state.producerId(),
                                    state.producerEpoch(),
                                    state.lastSequence(),
                                    state.lastTimestamp()))
                        .collect(Collectors.toList())));
    var tab = new Tab("producer");
    tab.setContent(pane);
    return tab;
  }

  public static class Bean {
    private final String topic;
    private final int partition;
    private final long producerId;
    private final int producerEpoch;
    private final int lastSequence;
    private final long lastTimestamp;

    public Bean(
        String topic,
        int partition,
        long producerId,
        int producerEpoch,
        int lastSequence,
        long lastTimestamp) {
      this.topic = topic;
      this.partition = partition;
      this.producerId = producerId;
      this.producerEpoch = producerEpoch;
      this.lastSequence = lastSequence;
      this.lastTimestamp = lastTimestamp;
    }
  }
}
