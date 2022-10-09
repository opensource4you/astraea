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

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Objects;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;

public class HttpExecutorBuilder {

  JsonConverter jsonConverter;

  public HttpExecutorBuilder() {
    this.jsonConverter = JsonConverter.gson();
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
      public <T> HttpResponse<T> get(String url, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();

              return toGsonHttpResponse(
                  client.send(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> HttpResponse<T> get(String url, Object param, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .GET()
                      .uri(Utils.getQueryUrl(new URL(url), object2Map(param)).toURI())
                      .build();

              return toGsonHttpResponse(
                  client.send(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> HttpResponse<T> get(String url, Type type) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(url)).build();

              return toGsonHttpResponse(
                  client.send(request, HttpResponse.BodyHandlers.ofString()), type);
            });
      }

      @Override
      public <T> HttpResponse<T> post(String url, Object body, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .POST(gsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return toGsonHttpResponse(
                  client.send(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public <T> HttpResponse<T> put(String url, Object body, Class<T> respCls) {
        return Utils.packException(
            () -> {
              HttpRequest request =
                  HttpRequest.newBuilder()
                      .PUT(gsonRequestHandler(body))
                      .header("Content-type", "application/json")
                      .uri(new URI(url))
                      .build();

              return toGsonHttpResponse(
                  client.send(request, HttpResponse.BodyHandlers.ofString()), respCls);
            });
      }

      @Override
      public HttpResponse<Void> delete(String url) {
        return Utils.packException(
            () -> {
              HttpRequest request = HttpRequest.newBuilder().DELETE().uri(new URI(url)).build();
              return withException(client.send(request, HttpResponse.BodyHandlers.discarding()));
            });
      }

      private <T> HttpResponse<T> toGsonHttpResponse(HttpResponse<String> response, Type type)
          throws StringResponseException {
        var innerResponse = withException(response);
        return new MappedHttpResponse<>(
            response,
            x -> {
              if (Objects.requireNonNull(x).isBlank()) {
                throw new StringResponseException(response, type);
              }

              try {
                return jsonConverter.fromJson(x, type);
              } catch (JsonSyntaxException jsonSyntaxException) {
                throw new StringResponseException(response, type);
              }
            });
      }

      /**
       * if api return error code, then response type will be changed. So we throw an exception to
       * identify that error.
       */
      @SuppressWarnings("unchecked")
      private <T> HttpResponse<T> withException(HttpResponse<T> httpResponse)
          throws StringResponseException {

        if (httpResponse.statusCode() >= 400) {
          HttpResponse<String> stringHttpResponse;
          if (Objects.isNull(httpResponse.body())) {
            stringHttpResponse = new MappedHttpResponse<>(httpResponse, (x) -> null);
          } else if (httpResponse.body() instanceof String) {
            stringHttpResponse = (HttpResponse<String>) httpResponse;
          } else {
            stringHttpResponse = new MappedHttpResponse<>(httpResponse, Object::toString);
          }
          throw new StringResponseException(stringHttpResponse);
        } else {
          return httpResponse;
        }
      }

      private Map<String, String> object2Map(Object obj) {
        return jsonConverter.fromJson(
            jsonConverter.toJson(obj), new TypeToken<Map<String, String>>() {}.getType());
      }

      private <T> HttpRequest.BodyPublisher gsonRequestHandler(Object t) {
        return HttpRequest.BodyPublishers.ofString(jsonConverter.toJson(t));
      }
    };
  }
}
