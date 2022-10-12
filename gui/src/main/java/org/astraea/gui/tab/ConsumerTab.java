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
package org.astraea.gui.tab;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.ConsumerGroup;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class ConsumerTab {

  private static List<Map<String, Object>> result(Stream<ConsumerGroup> cgs, Input input) {
    return cgs.flatMap(
            cg ->
                cg.assignment().entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().stream()
                                .filter(
                                    tp ->
                                        input.matchSearch(tp.topic())
                                            || input.matchSearch(entry.getKey().groupId()))
                                .map(
                                    tp ->
                                        LinkedHashMap.<String, Object>of(
                                            "group",
                                            entry.getKey().groupId(),
                                            "coordinator",
                                            cg.coordinator().id(),
                                            "topic",
                                            tp.topic(),
                                            "partition",
                                            tp.partition(),
                                            "offset",
                                            Optional.ofNullable(cg.consumeProgress().get(tp))
                                                .map(String::valueOf)
                                                .orElse("unknown"),
                                            "client host",
                                            entry.getKey().host(),
                                            "client id",
                                            entry.getKey().clientId(),
                                            "member id",
                                            entry.getKey().memberId(),
                                            "instance id",
                                            entry.getKey().groupInstanceId().orElse("")))))
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .searchField("group id or topic name")
            .buttonAction(
                (input, logger) ->
                    context.submit(
                        admin ->
                            admin
                                .consumerGroupIds()
                                .thenCompose(admin::consumerGroups)
                                .thenApply(cgs -> result(cgs.stream(), input))))
            .build();
    return Tab.of("consumer", pane);
  }
}
