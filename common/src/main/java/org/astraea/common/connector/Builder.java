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
   * PluginInfo}, {@link WorkerInfo}
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
      public CompletionStage<WorkerInfo> info() {
        return httpExecutor
            .get(getURL("/"), TypeRef.of(WorkerInfo.class))
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
            .get(getURL(KEY_CONNECTORS + "/" + name + "/status"), TypeRef.of(ConnectorStatus.class))
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
}
