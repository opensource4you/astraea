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
package org.astraea.common.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public class HttpExecutorBuilder {

  JsonConverter jsonConverter = JsonConverter.defaultConverter();

  BiFunction<String, JsonConverter, String> errorMessageHandler = (body, converter) -> body;

  HttpExecutorBuilder() {}

  public HttpExecutorBuilder jsonConverter(JsonConverter jsonConverter) {
    this.jsonConverter = Objects.requireNonNull(jsonConverter);
    return this;
  }

  public HttpExecutorBuilder errorMessageHandler(
      BiFunction<String, JsonConverter, String> errorMessageHandler) {
    this.errorMessageHandler = Objects.requireNonNull(errorMessageHandler);
    return this;
  }

  public HttpExecutor build() {
    return new HttpExecutor() {
      private final JsonConverter jsonConverter = HttpExecutorBuilder.this.jsonConverter;
      private final HttpClient client = HttpClient.newHttpClient();

      private <T> CompletionStage<Response<T>> send(HttpRequest request, TypeRef<T> typeRef) {
        return client
            .sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(
                r -> {
                  if (r.statusCode() < 400) {
                    if (typeRef != null && r.body() == null)
                      throw new IllegalStateException("There is no body!!!");
                    if (typeRef == null) return Response.of(null, r.statusCode());
                    return Response.of(jsonConverter.fromJson(r.body(), typeRef), r.statusCode());
                  }
                  if (r.body() == null || r.body().isBlank())
                    throw new HttpRequestException(r.statusCode());
                  throw new HttpRequestException(
                      r.statusCode(), errorMessageHandler.apply(r.body(), jsonConverter));
                });
      }

      @Override
      public <T> CompletionStage<Response<T>> get(String url, TypeRef<T> typeRef) {
        return send(HttpRequest.newBuilder().GET().uri(uri(url, Map.of())).build(), typeRef);
      }

      @Override
      public <T> CompletionStage<Response<T>> get(
          String url, Map<String, String> param, TypeRef<T> typeRef) {
        return send(HttpRequest.newBuilder().GET().uri(uri(url, param)).build(), typeRef);
      }

      @Override
      public <T> CompletionStage<Response<T>> post(String url, Object body, TypeRef<T> typeRef) {
        return send(
            HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(jsonConverter.toJson(body)))
                .header("Content-type", "application/json")
                .uri(uri(url, Map.of()))
                .build(),
            typeRef);
      }

      @Override
      public <T> CompletionStage<Response<T>> put(String url, Object body, TypeRef<T> typeRef) {
        return send(
            HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofString(jsonConverter.toJson(body)))
                .header("Content-type", "application/json")
                .uri(uri(url, Map.of()))
                .build(),
            typeRef);
      }

      @Override
      public CompletionStage<Response<Void>> delete(String url) {
        return send(HttpRequest.newBuilder().DELETE().uri(uri(url, Map.of())).build(), null);
      }
    };
  }

  static URI uri(String url, Map<String, String> parameters) {
    try {
      var uri = new URI(url);
      var queryString =
          parameters.entrySet().stream()
              .map(
                  x -> {
                    var key = x.getKey();
                    return key + "=" + URLEncoder.encode(x.getValue(), StandardCharsets.UTF_8);
                  })
              .collect(Collectors.joining("&"));

      return new URI(
          uri.getScheme(), uri.getAuthority(), uri.getPath(), queryString, uri.getFragment());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
