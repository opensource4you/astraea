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
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public class HttpExecutorBuilder {

  JsonConverter jsonConverter;

  public HttpExecutorBuilder() {
    this.jsonConverter = JsonConverter.defaultConverter();
  }

  public HttpExecutorBuilder jsonConverter(JsonConverter jsonConverter) {
    this.jsonConverter = jsonConverter;
    return this;
  }

  public HttpExecutor build() {
    var client = HttpClient.newHttpClient();
    var jsonConverter = this.jsonConverter;

    return new HttpExecutor() {
      @Override
      public <T> CompletionStage<Response<T>> get(String url, TypeRef<T> typeRef) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();
              return client
                  .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                  .thenApply(stringHttpResponse -> toJsonHttpResponse(stringHttpResponse, typeRef))
                  .thenApply(Response::of);
            });
      }

      @Override
      public <T> CompletionStage<Response<T>> get(
          String url, Map<String, String> param, TypeRef<T> typeRef) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder().GET().uri(getParameterURI(url, param)).build();

              return client
                  .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                  .thenApply(stringHttpResponse -> toJsonHttpResponse(stringHttpResponse, typeRef))
                  .thenApply(Response::of);
            });
      }

      @Override
      public <T> CompletionStage<Response<T>> post(String url, Object body, TypeRef<T> typeRef) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .POST(jsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return client
                  .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                  .thenApply(stringHttpResponse -> toJsonHttpResponse(stringHttpResponse, typeRef))
                  .thenApply(Response::of);
            });
      }

      @Override
      public <T> CompletionStage<Response<T>> put(String url, Object body, TypeRef<T> typeRef) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .PUT(jsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return client
                  .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                  .thenApply(stringHttpResponse -> toJsonHttpResponse(stringHttpResponse, typeRef))
                  .thenApply(Response::of);
            });
      }

      @Override
      public CompletionStage<Response<Void>> delete(String url) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().DELETE().uri(new URI(url)).build();
              return client
                  .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                  .thenApply(this::toVoidHttpResponse)
                  .thenApply(Response::of);
            });
      }

      /** handle json or void {@link #toVoidHttpResponse(HttpResponse)} */
      private <T> HttpResponse<T> toJsonHttpResponse(
          HttpResponse<String> response, TypeRef<T> typeRef) throws StringResponseException {
        var innerResponse = withException(response);
        return new MappedHttpResponse<>(
            innerResponse,
            x -> {
              try {
                return jsonConverter.fromJson(x, typeRef);
              } catch (Exception e) {
                throw new StringResponseException(innerResponse, typeRef.getType(),e);
              }
            });
      }

      /** handle void or json {@link #toJsonHttpResponse(HttpResponse, TypeRef)} (HttpResponse)} */
      private HttpResponse<Void> toVoidHttpResponse(HttpResponse<String> response) {
        var innerResponse = withException(response);
        return new MappedHttpResponse<>(innerResponse, x -> null);
      }

      private <T> HttpResponse<T> withException(HttpResponse<T> response) {
        if (response.statusCode() >= 400) {
          throw new StringResponseException(toStringResponse(response));
        } else {
          return response;
        }
      }

      /**
       * if api return error code, then response type will be changed. So we throw an exception to
       * identify that error.
       */
      @SuppressWarnings("unchecked")
      private <T> HttpResponse<String> toStringResponse(HttpResponse<T> httpResponse)
          throws StringResponseException {
        HttpResponse<String> stringHttpResponse;
        if (Objects.isNull(httpResponse.body())) {
          stringHttpResponse = new MappedHttpResponse<>(httpResponse, (x) -> null);
        } else if (httpResponse.body() instanceof String) {
          stringHttpResponse = (HttpResponse<String>) httpResponse;
        } else {
          stringHttpResponse = new MappedHttpResponse<>(httpResponse, Object::toString);
        }
        throw new StringResponseException(stringHttpResponse);
      }

      private URI getParameterURI(String url, Map<String, String> parameter)
          throws URISyntaxException {
        return getQueryUri(url, parameter);
      }

      private HttpRequest.BodyPublisher jsonRequestHandler(Object t) {
        return HttpRequest.BodyPublishers.ofString(jsonConverter.toJson(t));
      }
    };
  }

  static URI getQueryUri(String url, Map<String, String> parameters) throws URISyntaxException {
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
  }
}
