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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import javafx.geometry.Side;
import javafx.scene.Node;
import org.astraea.common.FutureUtils;
import org.astraea.common.MapUtils;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.connector.Definition;
import org.astraea.common.connector.WorkerStatus;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.connector.ConnectorMetrics;
import org.astraea.gui.Context;
import org.astraea.gui.button.SelectBox;
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
                TextInput.required(ConnectorConfigs.NAME_KEY, EditableText.singleLine().build()),
                TextInput.required(
                    ConnectorConfigs.CONNECTOR_CLASS_KEY, EditableText.singleLine().build()),
                TextInput.required(ConnectorConfigs.TOPICS_KEY, EditableText.singleLine().build()),
                TextInput.required(
                    ConnectorConfigs.TASK_MAX_KEY, EditableText.singleLine().onlyNumber().build()),
                TextInput.of(
                    ConnectorConfigs.KEY_CONVERTER_KEY,
                    EditableText.singleLine()
                        .hint(ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS)
                        .build()),
                TextInput.of(
                    ConnectorConfigs.VALUE_CONVERTER_KEY,
                    EditableText.singleLine()
                        .hint(ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS)
                        .build()),
                TextInput.of(
                    ConnectorConfigs.HEADER_CONVERTER_KEY,
                    EditableText.singleLine()
                        .hint(ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS)
                        .build()),
                TextInput.of("configs", EditableText.multiline().hint("{\"a\":\"b\"}").build())),
            "CREATE",
            (argument, logger) -> {
              var req = new HashMap<>(argument.nonEmptyTexts());
              var configs = req.remove("configs");
              if (configs != null)
                req.putAll(
                    JsonConverter.defaultConverter().fromJson(configs, TypeRef.map(String.class)));
              return context
                  .connectorClient()
                  .createConnector(req.remove(ConnectorConfigs.NAME_KEY), req)
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
                                context.workerClients().values().stream()
                                    .flatMap(c -> ConnectorMetrics.sourceTaskInfo(c).stream())
                                    .collect(Collectors.toList())),
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
                                context.workerClients().values().stream()
                                    .flatMap(c -> ConnectorMetrics.sinkTaskInfo(c).stream())
                                    .collect(Collectors.toList())),
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
                                context.workerClients().values().stream()
                                    .flatMap(c -> ConnectorMetrics.taskError(c).stream())
                                    .collect(Collectors.toList())),
                    (connectorStatuses, sourceTaskMetrics, sinkTaskMetrics, taskErrors) ->
                        connectorStatuses.stream()
                            .flatMap(
                                connectorStatus ->
                                    connectorStatus.tasks().stream()
                                        .map(
                                            task -> {
                                              var map = new LinkedHashMap<String, Object>();
                                              map.put(NAME_KEY, task.connectorName());
                                              map.put("id", task.id());
                                              map.put("worker id", task.workerId());
                                              map.put("state", task.state());
                                              connectorStatus
                                                  .type()
                                                  .ifPresent(t -> map.put("type", t));
                                              task.error().ifPresent(e -> map.put("error", e));
                                              map.putAll(task.configs());
                                              sourceTaskMetrics.stream()
                                                  .filter(t -> t.taskId() == task.id())
                                                  .filter(
                                                      t ->
                                                          t.connectorName()
                                                              .equals(connectorStatus.name()))
                                                  .forEach(
                                                      info -> {
                                                        map.put(
                                                            "poll rate (records/s)",
                                                            info.sourceRecordPollRate());
                                                        map.put(
                                                            "records",
                                                            info.sourceRecordPollTotal());
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
                                                      });
                                              sinkTaskMetrics.stream()
                                                  .filter(t -> t.taskId() == task.id())
                                                  .filter(
                                                      t ->
                                                          t.connectorName()
                                                              .equals(connectorStatus.name()))
                                                  .forEach(
                                                      info -> {
                                                        map.put(
                                                            "partitions", info.partitionCount());
                                                        map.put(
                                                            "put batch time (avg)",
                                                            info.putBatchAvgTimeMs());
                                                        map.put(
                                                            "put batch time (max)",
                                                            info.putBatchMaxTimeMs());
                                                        map.put(
                                                            "read rate (records/s)",
                                                            info.sinkRecordReadRate());
                                                        map.put(
                                                            "read records",
                                                            info.sinkRecordReadTotal());
                                                      });
                                              taskErrors.stream()
                                                  .filter(t -> t.taskId() == task.id())
                                                  .filter(
                                                      t ->
                                                          t.connectorName()
                                                              .equals(connectorStatus.name()))
                                                  .forEach(
                                                      error -> {
                                                        map.put("retries", error.totalRetries());
                                                        map.put(
                                                            "failure records",
                                                            error.totalRecordFailures());
                                                        map.put(
                                                            "skipped records",
                                                            error.totalRecordsSkipped());
                                                        map.put(
                                                            "error records",
                                                            error.totalRecordErrors());
                                                      });
                                              return map;
                                            }))
                            .collect(Collectors.toList())))
        .build();
  }

  private static Node pluginNode(Context context) {
    var documentation = "documentation";
    var defaultValue = "default value";
    return PaneBuilder.of()
        .firstPart(
            SelectBox.single(List.of(defaultValue, documentation), 2),
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
                                      map.put("class", pluginInfo.className());
                                      pluginInfo.definitions().stream()
                                          .sorted(Comparator.comparing(Definition::name))
                                          .forEach(
                                              d ->
                                                  map.put(
                                                      d.name(),
                                                      argument
                                                              .selectedKeys()
                                                              .contains(documentation)
                                                          ? d.documentation()
                                                          : d.defaultValue().orElse("")));
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
