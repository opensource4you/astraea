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
import java.util.Optional;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;

public class ConsumerTab {
  public static Tab of(Context context) {
    var pane =
        Utils.searchToTable(
            "search for consumer groups:",
            word ->
                context
                    .optionalAdmin()
                    .map(
                        admin ->
                            admin
                                .consumerGroups(
                                    admin.consumerGroupIds().stream()
                                        .filter(group -> word.isEmpty() || group.contains(word))
                                        .collect(Collectors.toSet()))
                                .stream()
                                .flatMap(
                                    cg ->
                                        cg.assignment().entrySet().stream()
                                            .flatMap(
                                                entry ->
                                                    entry.getValue().stream()
                                                        .map(
                                                            tp ->
                                                                LinkedHashMap.of(
                                                                    "group",
                                                                        entry.getKey().groupId(),
                                                                    "topic", tp.topic(),
                                                                    "partition",
                                                                        String.valueOf(
                                                                            tp.partition()),
                                                                    "offset",
                                                                        Optional.ofNullable(
                                                                                cg.consumeProgress()
                                                                                    .get(tp))
                                                                            .map(String::valueOf)
                                                                            .orElse("unknown"),
                                                                    "client host",
                                                                        entry.getKey().host(),
                                                                    "client id",
                                                                        entry.getKey().clientId(),
                                                                    "member id",
                                                                        entry.getKey().memberId(),
                                                                    "instance id",
                                                                        entry
                                                                            .getKey()
                                                                            .groupInstanceId()
                                                                            .orElse("")))))
                                .collect(Collectors.toList()))
                    .orElse(List.of()));
    var tab = new Tab("consumer");
    tab.setContent(pane);
    return tab;
  }
}
