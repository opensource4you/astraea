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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorInfo;
import org.astraea.gui.Context;
import org.astraea.gui.pane.MultiInput;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class ConnectorNode {

  private static Node createNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            MultiInput.of(
                List.of(
                    TextInput.required(ConnectorClient.NAME_KEY, EditableText.singleLine().build()),
                    TextInput.required(
                        ConnectorClient.CONNECTOR_CLASS_KEY, EditableText.singleLine().build()),
                    TextInput.required(
                        ConnectorClient.TOPICS_KEY, EditableText.singleLine().build()),
                    TextInput.required(
                        ConnectorClient.TASK_MAX_KEY,
                        EditableText.singleLine().onlyNumber().build()),
                    TextInput.of("configs", EditableText.multiline().build()))),
            "CREATE",
            (argument, logger) -> {
              var req = new HashMap<>(argument.nonEmptyTexts());
              return context
                  .connectorClient()
                  .createConnector(req.remove(ConnectorClient.NAME_KEY), req)
                  .thenApply(connectorInfo -> result(List.of(connectorInfo)));
            })
        .build();
  }

  private static Node taskNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                context
                    .connectorClient()
                    .connectorNames()
                    .thenCompose(
                        names ->
                            FutureUtils.sequence(
                                names.stream()
                                    .map(
                                        name ->
                                            context
                                                .connectorClient()
                                                .connectorStatus(name)
                                                .toCompletableFuture())
                                    .collect(Collectors.toList())))
                    .thenApply(
                        connectorStatuses ->
                            connectorStatuses.stream()
                                .flatMap(
                                    connectorStatus ->
                                        connectorStatus.tasks().stream()
                                            .map(
                                                task -> {
                                                  var map = new LinkedHashMap<String, Object>();
                                                  map.put("name", connectorStatus.name());
                                                  map.put("id", task.id());
                                                  map.put("worker id", task.workerId());
                                                  map.put("state", task.state());
                                                  task.trace().ifPresent(e -> map.put("error", e));
                                                  return map;
                                                }))
                                .collect(Collectors.toList())))
        .build();
  }

  private static Node pluginNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                context
                    .connectorClient()
                    .plugins()
                    .thenApply(
                        pluginInfos ->
                            pluginInfos.stream()
                                .map(
                                    pluginInfo -> {
                                      var map = new LinkedHashMap<String, Object>();
                                      map.put("class name", pluginInfo.className());
                                      return map;
                                    })
                                .collect(Collectors.toList())))
        .build();
  }

  private static Node basicNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                context
                    .connectorClient()
                    .connectorNames()
                    .thenCompose(
                        names ->
                            FutureUtils.sequence(
                                names.stream()
                                    .map(
                                        name ->
                                            context
                                                .connectorClient()
                                                .connectorInfo(name)
                                                .toCompletableFuture())
                                    .collect(Collectors.toList())))
                    .thenApply(ConnectorNode::result))
        .build();
  }

  private static List<Map<String, Object>> result(List<ConnectorInfo> connectorInfos) {
    return connectorInfos.stream()
        .map(
            connectorInfo -> {
              var map = new LinkedHashMap<String, Object>();
              map.put("name", connectorInfo.name());
              map.put(
                  "tasks",
                  connectorInfo.tasks().stream()
                      .map(t -> String.valueOf(t.id()))
                      .collect(Collectors.joining(",")));
              map.putAll(connectorInfo.config());
              return map;
            })
        .collect(Collectors.toList());
  }

  public static Node of(Context context) {
    return Slide.of(
            Side.TOP,
            MapUtils.of(
                "basic",
                basicNode(context),
                "task",
                taskNode(context),
                "create",
                createNode(context),
                "plugin",
                pluginNode(context)))
        .node();
  }
}
