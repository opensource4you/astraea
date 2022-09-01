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
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.common.EnumInfo;
import org.astraea.app.common.Utils;

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

  static Channel ofRequest(PostRequest request) {
    return builder().type(Type.POST).request(request).build();
  }

  class Builder {
    private Type type = Type.UNKNOWN;
    private Optional<String> target = Optional.empty();
    private Map<String, String> queries = Map.of();
    private PostRequest request = PostRequest.EMPTY;
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

    public Builder request(Map<String, Object> request) {
      return request(PostRequest.of(request));
    }

    public Builder request(PostRequest request) {
      this.request = request;
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
        public PostRequest request() {
          return request;
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
    UNKNOWN;

    public static Type ofAlias(String alias) {
      return Utils.ignoreCaseEnum(Type.class, alias);
    }
  }

  static Channel of(HttpExchange exchange) {
    Function<URI, Optional<String>> parseTarget =
        uri -> {
          var allPaths =
              Arrays.stream(uri.getPath().split("/"))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .collect(Collectors.toUnmodifiableList());
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

    Function<InputStream, PostRequest> parseRequest =
        stream -> {
          var bs = Utils.packException(stream::readAllBytes);
          if (bs == null || bs.length == 0) return PostRequest.EMPTY;
          return PostRequest.of(new String(bs, StandardCharsets.UTF_8));
        };

    Function<String, Type> parseType =
        name -> {
          switch (name.toUpperCase(Locale.ROOT)) {
            case "GET":
              return Type.GET;
            case "POST":
              return Type.POST;
            case "DELETE":
              return Type.DELETE;
            default:
              return Type.UNKNOWN;
          }
        };
    return builder()
        .type(parseType.apply(exchange.getRequestMethod()))
        .target(parseTarget.apply(exchange.getRequestURI()))
        .queries(parseQueries.apply(exchange.getRequestURI()))
        .request(parseRequest.apply(exchange.getRequestBody()))
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

  /** @return the type of HTTP method */
  Type type();

  /** @return the target from URL. The form is /{type}/target */
  Optional<String> target();

  /** @return body request */
  PostRequest request();

  /** @return the queries appended to URL */
  Map<String, String> queries();

  /**
   * send the response to caller and then complete the response
   *
   * @param response to send back to caller
   */
  void send(Response response);
}
