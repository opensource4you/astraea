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
package org.astraea.common.connector;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.http.HttpExecutor;
import org.astraea.common.http.Response;
import org.astraea.common.json.TypeRef;

public class Builder {

  private static final String KEY_CONNECTORS = "/connectors";

  // TODO: 2022-12-02 astraea-1200 handler error url
  private List<URL> urls = List.of();
  private HttpExecutor builderHttpExecutor;

  public Builder urls(Set<URL> urls) {
    this.urls = new ArrayList<>(Objects.requireNonNull(urls));
    return this;
  }

  public Builder url(URL url) {
    this.urls = List.of(Objects.requireNonNull(url));
    return this;
  }

  /**
   * custom httpExecutor, please make sure that the httpExecutor can convert some special {@link
   * TypeRef} of beans whose response contains java keyword or kebab-case For example, {@link
   * PluginInfo}
   */
  public Builder httpExecutor(HttpExecutor httpExecutor) {
    this.builderHttpExecutor = Objects.requireNonNull(httpExecutor);
    return this;
  }

  public ConnectorClient build() {
    if (urls.isEmpty()) {
      throw new IllegalArgumentException("Urls should be set.");
    }

    var httpExecutor =
        Optional.ofNullable(builderHttpExecutor)
            .orElseGet(
                () ->
                    HttpExecutor.builder()
                        .errorMessageHandler(
                            (body, converter) ->
                                converter.fromJson(body, TypeRef.of(WorkerError.class)).message())
                        .build());

    return new ConnectorClient() {

      @Override
      public CompletionStage<List<WorkerStatus>> activeWorkers() {
        Function<URL, String> workerId = url -> url.getHost() + ":" + url.getPort();
        return connectorNames()
            .thenCompose(
                names ->
                    FutureUtils.sequence(
                        names.stream()
                            .map(name -> connectorStatus(name).toCompletableFuture())
                            .collect(Collectors.toList())))
            .thenCompose(
                connectorStatuses ->
                    FutureUtils.sequence(
                        connectorStatuses.stream()
                            .flatMap(
                                s ->
                                    Stream.concat(
                                        s.tasks().stream().map(TaskStatus::workerId),
                                        Stream.of(s.workerId())))
                            .distinct()
                            // worker id is [worker host][worker port]
                            .filter(s -> s.split(":").length == 2)
                            .map(
                                s ->
                                    Utils.packException(
                                        () ->
                                            new URL(
                                                "http://"
                                                    + s.split(":")[0]
                                                    + ":"
                                                    + s.split(":")[1])))
                            .map(
                                url ->
                                    info(url)
                                        .thenApply(
                                            info ->
                                                new WorkerStatus(
                                                    url.getHost(),
                                                    url.getPort(),
                                                    info.version,
                                                    info.commit,
                                                    info.kafkaClusterId,
                                                    connectorStatuses.stream()
                                                        .filter(
                                                            s ->
                                                                s.workerId()
                                                                    .equals(workerId.apply(url)))
                                                        .count(),
                                                    connectorStatuses.stream()
                                                        .flatMap(s -> s.tasks().stream())
                                                        .filter(
                                                            s ->
                                                                s.workerId()
                                                                    .equals(workerId.apply(url)))
                                                        .count()))
                                        .toCompletableFuture())
                            .collect(Collectors.toList())));
      }

      private CompletionStage<KafkaWorkerInfo> info(URL url) {
        return httpExecutor
            .get(url.toString() + "/", TypeRef.of(KafkaWorkerInfo.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<Set<String>> connectorNames() {
        return httpExecutor
            .get(getURL(KEY_CONNECTORS), TypeRef.set(String.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<ConnectorInfo> connectorInfo(String name) {
        return httpExecutor
            .get(getURL(KEY_CONNECTORS + "/" + name), TypeRef.of(ConnectorInfo.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<ConnectorStatus> connectorStatus(String name) {
        return httpExecutor
            .get(
                getURL(KEY_CONNECTORS + "/" + name + "/status"),
                TypeRef.of(KafkaConnectorStatus.class))
            .thenApply(Response::body)
            .thenCompose(
                status ->
                    config(name)
                        .thenApply(
                            config ->
                                new ConnectorStatus(
                                    status.name,
                                    status.connector.state,
                                    status.connector.worker_id,
                                    config,
                                    status.tasks.stream()
                                        .map(
                                            t ->
                                                new TaskStatus(t.id, t.state, t.worker_id, t.trace))
                                        .collect(Collectors.toList()))));
      }

      private CompletionStage<Map<String, String>> config(String name) {
        return httpExecutor
            .get(getURL(KEY_CONNECTORS + "/" + name + "/config"), TypeRef.map(String.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<ConnectorInfo> createConnector(
          String name, Map<String, String> config) {
        var connectorReq = new ConnectorReq(name, config);
        return httpExecutor
            .post(getURL(KEY_CONNECTORS), connectorReq, TypeRef.of(ConnectorInfo.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<ConnectorInfo> updateConnector(
          String name, Map<String, String> config) {
        return httpExecutor
            .put(
                getURL(KEY_CONNECTORS + "/" + name + "/config"),
                config,
                TypeRef.of(ConnectorInfo.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<Void> deleteConnector(String name) {
        return httpExecutor.delete(getURL(KEY_CONNECTORS + "/" + name)).thenApply(Response::body);
      }

      @Override
      public CompletionStage<Validation> validate(String name, Map<String, String> configs) {
        return httpExecutor
            .put(
                getURL("/connector-plugins") + "/" + name + "/config/validate",
                configs,
                TypeRef.of(Validation.class))
            .thenApply(Response::body);
      }

      @Override
      public CompletionStage<Set<PluginInfo>> plugins() {
        return httpExecutor
            .get(getURL("/connector-plugins"), TypeRef.set(PluginInfo.class))
            .thenApply(Response::body);
      }

      private String getURL(String path) {
        try {
          var index = ThreadLocalRandom.current().nextInt(0, urls.size());
          return urls.get(index).toURI().resolve(path).toString();
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
      }
    };
  }

  private static class KafkaWorkerInfo {
    private String version;
    private String commit;

    @JsonAlias("kafka_cluster_id")
    private String kafkaClusterId;
  }

  private static class KafkaConnectorStatus {
    private String name;

    private KafkaConnector connector;

    private List<KafkaTaskStatus> tasks;
  }

  private static class KafkaConnector {
    private String state;
    private String worker_id;
  }

  private static class KafkaTaskStatus {
    private int id;
    private String state;

    private String worker_id;

    private Optional<String> trace = Optional.empty();
  }
}
