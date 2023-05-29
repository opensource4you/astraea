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

import com.sun.net.httpserver.HttpExchange;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

interface Channel {

  Channel EMPTY = Channel.builder().build();

  static Builder builder() {
    return new Builder();
  }

  static Channel ofTarget(String target) {
    return builder().type(Type.GET).target(target).build();
  }

  static Channel ofQueries(String target, Map<String, String> queries) {
    return builder().type(Type.GET).target(target).queries(queries).build();
  }

  static Channel ofQueries(Map<String, String> queries) {
    return builder().type(Type.GET).queries(queries).build();
  }

  static Channel ofRequest(String json) {
    return builder().type(Type.POST).request(json).build();
  }

  class Builder {
    private Type type = Type.UNKNOWN;
    private Optional<String> target = Optional.empty();
    private Map<String, String> queries = Map.of();
    private Optional<String> body = Optional.empty();
    private Consumer<Response> sender = r -> {};

    private Builder() {}

    public Builder type(Type type) {
      this.type = type;
      return this;
    }

    public Builder target(String target) {
      return target(Optional.of(target));
    }

    public Builder target(Optional<String> target) {
      this.target = target;
      return this;
    }

    public Builder queries(Map<String, String> queries) {
      this.queries = queries;
      return this;
    }

    public Builder request(String json) {
      this.body = Optional.ofNullable(json);
      return this;
    }

    public Builder sender(Consumer<Response> sender) {
      this.sender = sender;
      return this;
    }

    public Channel build() {
      return new Channel() {
        @Override
        public Type type() {
          return type;
        }

        @Override
        public Optional<String> target() {
          return target;
        }

        @Override
        public <T extends Request> T request(TypeRef<T> typeRef) {
          var json = body.orElse("{}");
          return JsonConverter.defaultConverter().fromJson(json, typeRef);
        }

        @Override
        public Map<String, String> queries() {
          return queries;
        }

        @Override
        public void send(Response response) {
          try {
            sender.accept(response);
            response.onComplete(null);
          } catch (Throwable e) {
            e.printStackTrace();
            response.onComplete(e);
          }
        }
      };
    }
  }

  enum Type implements EnumInfo {
    GET,
    DELETE,
    POST,
    PUT,
    UNKNOWN;

    public static Type ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Type.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }

  static Channel of(HttpExchange exchange) {
    Function<URI, Optional<String>> parseTarget =
        uri -> {
          var allPaths =
              Arrays.stream(uri.getPath().split("/"))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .toList();
          // form: /resource/target
          if (allPaths.size() == 1) return Optional.empty();
          else if (allPaths.size() == 2) return Optional.of(allPaths.get(1));
          else throw new IllegalArgumentException("unsupported url: " + uri);
        };

    Function<URI, Map<String, String>> parseQueries =
        uri -> {
          if (uri.getQuery() == null || uri.getQuery().isEmpty()) return Map.of();
          return Arrays.stream(uri.getQuery().split("&"))
              .filter(pair -> pair.split("=").length == 2)
              .collect(Collectors.toMap(p -> p.split("=")[0], p -> p.split("=")[1]));
        };

    Function<byte[], String> parseRequest =
        bs -> {
          if (bs == null || bs.length == 0) return null;
          return new String(bs, StandardCharsets.UTF_8);
        };

    Function<String, Type> parseType =
        name ->
            switch (name.toUpperCase(Locale.ROOT)) {
              case "GET" -> Type.GET;
              case "POST" -> Type.POST;
              case "DELETE" -> Type.DELETE;
              case "PUT" -> Type.PUT;
              default -> Type.UNKNOWN;
            };

    // TODO: there is a temporary needed for reading the network stream twice
    //  remove this hack in future
    byte[] requestBytes = Utils.packException(() -> exchange.getRequestBody().readAllBytes());
    return builder()
        .type(parseType.apply(exchange.getRequestMethod()))
        .target(parseTarget.apply(exchange.getRequestURI()))
        .queries(parseQueries.apply(exchange.getRequestURI()))
        .request(parseRequest.apply(requestBytes))
        .sender(
            response -> {
              var responseData = response.json().getBytes(StandardCharsets.UTF_8);
              exchange
                  .getResponseHeaders()
                  .set(
                      "Content-Type",
                      String.format("application/json; charset=%s", StandardCharsets.UTF_8));
              Utils.packException(
                  () -> {
                    exchange.sendResponseHeaders(response.code(), responseData.length);
                    try (var os = exchange.getResponseBody()) {
                      os.write(responseData);
                    }
                  });
            })
        .build();
  }

  /**
   * @return the type of HTTP method
   */
  Type type();

  /**
   * @return the target from URL. The form is /{type}/target
   */
  Optional<String> target();

  /**
   * @return body request
   */
  <T extends Request> T request(TypeRef<T> typeRef);

  /**
   * @return the queries appended to URL
   */
  Map<String, String> queries();

  /**
   * send the response to caller and then complete the response
   *
   * @param response to send back to caller
   */
  void send(Response response);
}
