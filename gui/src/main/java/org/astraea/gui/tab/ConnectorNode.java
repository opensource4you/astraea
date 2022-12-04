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

import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;

public class ConnectorNode {

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
                                                .connector(name)
                                                .toCompletableFuture())
                                    .collect(Collectors.toList())))
                    .thenApply(
                        connectorInfos ->
                            connectorInfos.stream()
                                .map(
                                    connectorInfo -> {
                                      var map = new LinkedHashMap<String, Object>();
                                      map.put("name", connectorInfo.name());
                                      map.put(
                                          "tasks",
                                          connectorInfo.tasks().stream()
                                              .map(t -> String.valueOf(t.taskId()))
                                              .collect(Collectors.joining(",")));
                                      map.putAll(connectorInfo.config());
                                      return map;
                                    })
                                .collect(Collectors.toList())))
        .build();
  }

  public static Node of(Context context) {
    return Slide.of(
            Side.TOP, MapUtils.of("basic", basicNode(context), "plugin", pluginNode(context)))
        .node();
  }
}
