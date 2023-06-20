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
package org.astraea.app.web;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.FutureUtils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.connector.ConnectorInfo;
import org.astraea.common.connector.ConnectorStatus;
import org.astraea.common.connector.TaskInfo;
import org.astraea.common.connector.TaskStatus;
import org.astraea.common.json.TypeRef;
import org.astraea.connector.backup.Exporter;
import org.astraea.connector.backup.Importer;

public class BackupHandler implements Handler {

  private final ConnectorClient connectorClient;

  BackupHandler(ConnectorClient connectorClient) {
    this.connectorClient = connectorClient;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    return channel
        .target()
        .map(name -> CompletableFuture.completedStage(Set.of(name)))
        .orElseGet(connectorClient::connectorNames)
        .thenCompose(
            names ->
                FutureUtils.sequence(
                    names.stream()
                        .map(name -> connectorClient.connectorStatus(name).toCompletableFuture())
                        .toList()))
        .thenApply(this::statusResponse);
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var postRequest = channel.request(TypeRef.of(BackupRequest.class));
    return FutureUtils.sequence(
            nameConfigEntry(postRequest)
                .map(
                    entry ->
                        connectorClient
                            .createConnector(entry.getKey(), entry.getValue())
                            .toCompletableFuture())
                .toList())
        .thenApply(this::infoResponse);
  }

  @Override
  public CompletionStage<Response> delete(Channel channel) {
    return channel
        .target()
        .map(name -> connectorClient.deleteConnector(name).thenApply(ignore -> Response.OK))
        .orElse(CompletableFuture.completedStage(Response.NOT_FOUND));
  }

  @Override
  public CompletionStage<Response> put(Channel channel) {
    var putRequest = channel.request(TypeRef.of(BackupRequest.class));
    return FutureUtils.sequence(
            nameConfigEntry(putRequest)
                .map(
                    entry ->
                        connectorClient
                            .updateConnector(entry.getKey(), entry.getValue())
                            .toCompletableFuture())
                .toList())
        .thenApply(this::infoResponse);
  }

  private Stream<Map.Entry<String, Map<String, String>>> nameConfigEntry(BackupRequest request) {
    return Stream.concat(
        request.importer.stream()
            .map(
                importer -> {
                  var config = connectorConfigMap(importer);
                  config.put(ConnectorConfigs.CONNECTOR_CLASS_KEY, importer.connectorClass);
                  config.put("clean.source", importer.cleanSourcePolicy);
                  importer.archiveDir.ifPresent(dir -> config.put("archive.dir", dir));
                  return Map.entry(importer.name, config);
                }),
        request.exporter.stream()
            .map(
                exporter -> {
                  var config = connectorConfigMap(exporter);
                  config.put(ConnectorConfigs.CONNECTOR_CLASS_KEY, exporter.connectorClass);
                  exporter.size.ifPresent(size -> config.put("size", size));
                  exporter.rollDuration.ifPresent(
                      duration -> config.put("roll.duration", duration));
                  exporter.writerBufferSize.ifPresent(
                      size -> config.put("writer.buffer.size", size));
                  exporter.offsetFrom.ifPresent(config::putAll);
                  return Map.entry(exporter.name, config);
                }));
  }

  private Map<String, String> connectorConfigMap(ConnectorConfig config) {
    var configMap = new HashMap<String, String>();
    configMap.put(ConnectorConfigs.TOPICS_KEY, config.topics);
    configMap.put(ConnectorConfigs.TASK_MAX_KEY, config.tasksMax);
    configMap.put(ConnectorConfigs.KEY_CONVERTER_KEY, config.keyConverter);
    configMap.put(ConnectorConfigs.VALUE_CONVERTER_KEY, config.valueConverter);
    configMap.put(ConnectorConfigs.HEADER_CONVERTER_KEY, config.headerConverter);
    configMap.put("path", config.path);
    configMap.put("fs.schema", config.fsSchema);
    config.hostname.ifPresent(
        hostname -> configMap.put("fs." + config.fsSchema + ".hostname", hostname));
    config.port.ifPresent(port -> configMap.put("fs." + config.fsSchema + ".port", port));
    config.user.ifPresent(user -> configMap.put("fs." + config.fsSchema + ".user", user));
    config.password.ifPresent(
        password -> configMap.put("fs." + config.fsSchema + ".password", password));
    return configMap;
  }

  private ConnectorInfoResponse infoResponse(List<ConnectorInfo> connectorInfos) {
    var groups =
        connectorInfos.stream()
            .map(
                connectorInfo ->
                    new ConnectorInfoClass(
                        connectorInfo.name(),
                        connectorInfo.config(),
                        connectorInfo.tasks().stream().map(TaskInfo::id).toList()))
            .collect(
                Collectors.groupingBy(
                    connectorInfoClass ->
                        connectorInfoClass.config.get(ConnectorConfigs.CONNECTOR_CLASS_KEY)));
    return new ConnectorInfoResponse(
        groups.get(Importer.class.getName()), groups.get(Exporter.class.getName()));
  }

  private ConnectorStatusResponse statusResponse(List<ConnectorStatus> connectorStatuses) {
    var groups =
        connectorStatuses.stream()
            .map(
                connectorStatus ->
                    new ConnectorStatusClass(
                        connectorStatus.name(),
                        connectorStatus.state(),
                        connectorStatus.configs(),
                        connectorStatus.tasks().stream()
                            .collect(Collectors.toMap(TaskStatus::id, TaskStatus::state))))
            .collect(
                Collectors.groupingBy(
                    connectorStatusClass ->
                        connectorStatusClass.configs.get(ConnectorConfigs.CONNECTOR_CLASS_KEY)));
    return new ConnectorStatusResponse(
        groups.get(Importer.class.getName()), groups.get(Exporter.class.getName()));
  }

  static class ConnectorStatusClass {
    final String name;
    final String state;
    final Map<String, String> configs;
    final Map<Integer, String> tasks;

    private ConnectorStatusClass() {
      this.name = "";
      this.state = "";
      this.configs = Map.of();
      this.tasks = Map.of();
    }

    private ConnectorStatusClass(
        String name, String state, Map<String, String> configs, Map<Integer, String> tasks) {
      this.name = name;
      this.state = state;
      this.configs = configs;
      this.tasks = tasks;
    }
  }

  static class ConnectorInfoClass {
    final String name;
    final Map<String, String> config;
    final List<Integer> tasks;

    private ConnectorInfoClass() {
      this.name = "";
      this.config = Map.of();
      this.tasks = List.of();
    }

    private ConnectorInfoClass(String name, Map<String, String> config, List<Integer> tasks) {
      this.name = name;
      this.config = config;
      this.tasks = tasks;
    }
  }

  static class ConnectorStatusResponse implements Response {
    final List<ConnectorStatusClass> importers;
    final List<ConnectorStatusClass> exporters;

    private ConnectorStatusResponse() {
      this.importers = List.of();
      this.exporters = List.of();
    }

    private ConnectorStatusResponse(
        List<ConnectorStatusClass> importers, List<ConnectorStatusClass> exporters) {
      this.importers = importers;
      this.exporters = exporters;
    }
  }

  static class ConnectorInfoResponse implements Response {
    final List<ConnectorInfoClass> importers;
    final List<ConnectorInfoClass> exporters;

    private ConnectorInfoResponse() {
      this.importers = List.of();
      this.exporters = List.of();
    }

    private ConnectorInfoResponse(
        List<ConnectorInfoClass> importers, List<ConnectorInfoClass> exporters) {
      this.importers = importers;
      this.exporters = exporters;
    }
  }

  static class ConnectorConfig implements Request {
    String name;
    String topics;
    String tasksMax;
    String path;
    String keyConverter = ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS;
    String valueConverter = ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS;
    String headerConverter = ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS;
    String fsSchema;
    Optional<String> hostname = Optional.empty();
    Optional<String> port = Optional.empty();
    Optional<String> user = Optional.empty();
    Optional<String> password = Optional.empty();
  }

  static class ImporterConfig extends ConnectorConfig implements Request {
    private final String connectorClass = Importer.class.getName();
    String cleanSourcePolicy;
    private Optional<String> archiveDir = Optional.empty();
  }

  static class ExporterConfig extends ConnectorConfig implements Request {
    private final String connectorClass = Exporter.class.getName();
    private Optional<String> size = Optional.empty();
    private Optional<String> rollDuration = Optional.empty();
    private Optional<String> writerBufferSize = Optional.empty();
    private Optional<Map<String, String>> offsetFrom = Optional.empty();
  }

  static class BackupRequest implements Request {
    List<ImporterConfig> importer = List.of();
    List<ExporterConfig> exporter = List.of();
  }
}
