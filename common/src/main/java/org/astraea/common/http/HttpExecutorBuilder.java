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

import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
      public <T> CompletableFuture<HttpResponse<T>> get(String url, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();
              return toGsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> CompletableFuture<HttpResponse<T>> get(
          String url, Object param, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .GET()
                      .uri(Utils.getQueryUri(url, object2Map(param)))
                      .build();

              return toGsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> CompletableFuture<HttpResponse<T>> get(String url, TypeRef<T> typeRef) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();

              return toGsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), typeRef);
            });
      }

      @Override
      public <T> CompletableFuture<HttpResponse<T>> post(
          String url, Object body, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .POST(gsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return toGsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> CompletableFuture<HttpResponse<T>> put(String url, Object body, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .PUT(gsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return toGsonHttpResponse(
                  client.sendAsync(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public CompletableFuture<HttpResponse<Void>> delete(String url) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().DELETE().uri(new URI(url)).build();
              return withException(
                  client.sendAsync(request, HttpResponse.BodyHandlers.discarding()));
            });
      }

      /**
       * if return value is Json , then we can convert it to Object. Or we just simply handle
       * exception by {@link #withException(CompletableFuture)}
       */
      private <T> CompletableFuture<HttpResponse<T>> toGsonHttpResponse(
          CompletableFuture<HttpResponse<String>> asyncResponse, TypeRef<T> typeRef)
          throws StringResponseException {
        return asyncResponse.thenApply(x -> toGsonHttpResponse(x, typeRef));
      }

      /**
       * if return value is Json , then we can convert it to Object. Or we just simply handle
       * exception by {@link #withException(CompletableFuture)}
       */
      private <T> CompletableFuture<HttpResponse<T>> toGsonHttpResponse(
          CompletableFuture<HttpResponse<String>> asyncResponse, Class<T> tClass)
          throws StringResponseException {
        return asyncResponse.thenApply(
            x ->
                toGsonHttpResponse(
                    x,
                    new TypeRef<>() {
                      @Override
                      public Type getType() {
                        return tClass;
                      }
                    }));
      }

      private <T> HttpResponse<T> toGsonHttpResponse(
          HttpResponse<String> response, TypeRef<T> typeRef) throws StringResponseException {
        var innerResponse = withException(response);
        return new MappedHttpResponse<>(
            innerResponse,
            x -> {
              if (Objects.requireNonNull(x).isBlank()) {
                throw new StringResponseException(innerResponse, typeRef.getType());
              }

              try {
                return jsonConverter.fromJson(x, typeRef);
              } catch (JsonSyntaxException jsonSyntaxException) {
                throw new StringResponseException(innerResponse, typeRef.getType());
              }
            });
      }

      /**
       * Handle exception with non json type response . If return value is json , we can use {@link
       * #toGsonHttpResponse(CompletableFuture, TypeRef)}
       */
      private <T> CompletableFuture<HttpResponse<T>> withException(
          CompletableFuture<HttpResponse<T>> response) {
        return response.thenApply(this::withException);
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

      private Map<String, String> object2Map(Object obj) {
        return jsonConverter.fromJson(jsonConverter.toJson(obj), new TypeRef<>() {});
      }

      private <T> HttpRequest.BodyPublisher gsonRequestHandler(Object t) {
        return HttpRequest.BodyPublishers.ofString(jsonConverter.toJson(t));
      }
    };
  }
}
