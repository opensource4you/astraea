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
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.WorkerStatus;
import org.astraea.common.metrics.connector.SourceMetrics;
import org.astraea.common.metrics.connector.SourceTaskError;
import org.astraea.common.metrics.connector.SourceTaskInfo;
import org.astraea.gui.Context;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Slide;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class ConnectorNode {

  private static final String NAME_KEY = "name";

  private static Node activeWorkerNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                context
                    .connectorClient()
                    .activeWorkers()
                    .thenApply(
                        workers ->
                            workers.stream()
                                .map(
                                    s -> {
                                      var map = new LinkedHashMap<String, Object>();
                                      map.put("hostname", s.hostname());
                                      map.put("port", s.port());
                                      map.put("connectors", s.numberOfConnectors());
                                      map.put("tasks", s.numberOfTasks());
                                      map.put("version", s.version());
                                      map.put("commit", s.commit());
                                      map.put("cluster id", s.kafkaClusterId());
                                      return map;
                                    })
                                .collect(Collectors.toList())))
        .build();
  }

  private static Node createNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            List.of(
                TextInput.required(ConnectorClient.NAME_KEY, EditableText.singleLine().build()),
                TextInput.required(
                    ConnectorClient.CONNECTOR_CLASS_KEY, EditableText.singleLine().build()),
                TextInput.required(ConnectorClient.TOPICS_KEY, EditableText.singleLine().build()),
                TextInput.required(
                    ConnectorClient.TASK_MAX_KEY, EditableText.singleLine().onlyNumber().build()),
                TextInput.of("configs", EditableText.multiline().build())),
            "CREATE",
            (argument, logger) -> {
              var req = new HashMap<>(argument.nonEmptyTexts());
              return context
                  .connectorClient()
                  .createConnector(req.remove(ConnectorClient.NAME_KEY), req)
                  .thenApply(
                      connectorInfo -> {
                        var map = new LinkedHashMap<String, Object>();
                        map.put(NAME_KEY, connectorInfo.name());
                        map.put(
                            "tasks",
                            connectorInfo.tasks().stream()
                                .map(t -> String.valueOf(t.id()))
                                .collect(Collectors.joining(",")));
                        map.putAll(connectorInfo.config());
                        return List.of(map);
                      });
            })
        .build();
  }

  private static Node taskNode(Context context) {
    return PaneBuilder.of()
        .firstPart(
            "REFRESH",
            (argument, logger) ->
                FutureUtils.combine(
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
                                        .collect(Collectors.toList()))),
                    // try to update metrics client of workers
                    context
                        .connectorClient()
                        .activeWorkers()
                        .thenApply(
                            workers ->
                                context.addWorkerClients(
                                    workers.stream()
                                        .map(WorkerStatus::hostname)
                                        .collect(Collectors.toSet())))
                        .thenApply(
                            ignored ->
                                Map.entry(
                                    context.workerClients().values().stream()
                                        .flatMap(c -> SourceMetrics.sourceTaskInfo(c).stream())
                                        .collect(
                                            Collectors.toMap(
                                                SourceTaskInfo::taskId, Function.identity())),
                                    context.workerClients().values().stream()
                                        .flatMap(c -> SourceMetrics.sourceTaskError(c).stream())
                                        .collect(
                                            Collectors.toMap(
                                                SourceTaskError::taskId, Function.identity())))),
                    (connectorStatuses, taskInfoAndErrors) ->
                        connectorStatuses.stream()
                            .flatMap(
                                connectorStatus ->
                                    connectorStatus.tasks().stream()
                                        .map(
                                            task -> {
                                              var map = new LinkedHashMap<String, Object>();
                                              map.put(NAME_KEY, connectorStatus.name());
                                              map.put("id", task.id());
                                              map.put("worker id", task.workerId());
                                              map.put("state", task.state());
                                              task.error().ifPresent(e -> map.put("error", e));
                                              var info = taskInfoAndErrors.getKey().get(task.id());
                                              if (info != null) {
                                                map.put(
                                                    "poll rate (records/s)",
                                                    info.sourceRecordPollRate());
                                                map.put("records", info.sourceRecordPollTotal());
                                                map.put(
                                                    "write rate (records/s)",
                                                    info.sourceRecordWriteRate());
                                                map.put(
                                                    "written records",
                                                    info.sourceRecordWriteTotal());
                                                map.put(
                                                    "poll batch time (avg)",
                                                    info.pollBatchAvgTimeMs());
                                                map.put(
                                                    "poll batch time (max)",
                                                    info.pollBatchMaxTimeMs());
                                              }
                                              var error =
                                                  taskInfoAndErrors.getValue().get(task.id());
                                              if (error != null) {
                                                map.put("retries", error.totalRetries());
                                                map.put(
                                                    "failure records", error.totalRecordFailures());
                                                map.put(
                                                    "skipped records", error.totalRecordsSkipped());
                                                map.put("error records", error.totalRecordErrors());
                                              }
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
                                                .connectorStatus(name)
                                                .toCompletableFuture())
                                    .collect(Collectors.toList())))
                    .thenApply(
                        connectorStatuses ->
                            connectorStatuses.stream()
                                .map(
                                    status -> {
                                      var map = new LinkedHashMap<String, Object>();
                                      map.put(NAME_KEY, status.name());
                                      map.put("state", status.state());
                                      map.put("worker id", status.workerId());
                                      map.put("tasks", status.tasks().size());
                                      map.putAll(status.configs());
                                      return map;
                                    })
                                .collect(Collectors.toList())))
        .secondPart(
            "DELETE",
            (tables, input, logger) ->
                FutureUtils.sequence(
                        tables.stream()
                            .filter(t -> t.containsKey(NAME_KEY))
                            .map(t -> t.get(NAME_KEY))
                            .map(
                                name ->
                                    context
                                        .connectorClient()
                                        .deleteConnector(name.toString())
                                        .toCompletableFuture())
                            .collect(Collectors.toList()))
                    .thenAccept(ignored -> logger.log("complete to delete connectors")))
        .build();
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
                "active workers",
                activeWorkerNode(context),
                "plugin",
                pluginNode(context)))
        .node();
  }
}
