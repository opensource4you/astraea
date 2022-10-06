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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.ProducerState;

public class ProducerTab {

  private static List<Map<String, Object>> result(Stream<ProducerState> states) {
    return states
        .map(
            state ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    state.topic(),
                    "partition",
                    state.partition(),
                    "producer id",
                    state.producerId(),
                    "producer epoch",
                    state.producerEpoch(),
                    "last sequence",
                    state.lastSequence(),
                    "last timestamp",
                    state.lastTimestamp()))
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    var pane =
        Utils.searchToTable(
            (word, console) ->
                context.submit(
                    admin ->
                        admin
                            .topicNames(true)
                            .thenApply(
                                names ->
                                    names.stream()
                                        .filter(name -> Utils.contains(name, word))
                                        .collect(Collectors.toSet()))
                            .thenCompose(admin::topicPartitions)
                            .thenCompose(admin::producerStates)
                            .thenApply(
                                ps ->
                                    result(
                                        ps.stream()
                                            .sorted(
                                                Comparator.comparing(ProducerState::topic)
                                                    .thenComparing(ProducerState::partition))))));
    var tab = new Tab("producer");
    tab.setContent(pane);
    return tab;
  }
}
