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
import org.astraea.common.admin.ProducerState;

public class ProducerTab {

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
                                        LinkedHashMap.of(
                                            "topic",
                                            state.topic(),
                                            "partition",
                                            String.valueOf(state.partition()),
                                            "producer id",
                                            String.valueOf(state.producerId()),
                                            "producer epoch",
                                            String.valueOf(state.producerEpoch()),
                                            "last sequence",
                                            String.valueOf(state.lastSequence()),
                                            "last timestamp",
                                            String.valueOf(state.lastTimestamp())))
                                .collect(Collectors.toList()))
                    .orElse(List.of()));
    var tab = new Tab("producer");
    tab.setContent(pane);
    return tab;
  }
}
