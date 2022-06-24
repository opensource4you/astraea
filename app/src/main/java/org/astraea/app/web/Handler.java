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
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

interface Handler extends HttpHandler {

  static <T> Set<T> compare(Set<T> all, Optional<T> target) {
    var nonexistent = target.stream().filter(id -> !all.contains(id)).collect(Collectors.toSet());
    if (!nonexistent.isEmpty()) throw new NoSuchElementException(nonexistent + " does not exist");
    return target.map(Set::of).orElse(all);
  }

  default Response process(HttpExchange exchange) {
    var method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
    var start = System.currentTimeMillis();
    try {
      switch (method) {
        case "GET":
          return get(parseTarget(exchange.getRequestURI()), parseQueries(exchange.getRequestURI()));
        case "POST":
          return post(PostRequest.of(exchange));
        case "DELETE":
          var target = parseTarget(exchange.getRequestURI());
          if (target.isPresent())
            return delete(target.get(), parseQueries(exchange.getRequestURI()));
        default:
          return Response.NOT_FOUND;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Response.of(e);
    } finally {
      System.out.println(
          "take "
              + (System.currentTimeMillis() - start)
              + "ms to process request from "
              + exchange.getRequestURI());
    }
  }

  @Override
  default void handle(HttpExchange exchange) throws IOException {
    Response response = process(exchange);
    var responseData = response.json().getBytes(StandardCharsets.UTF_8);
    exchange
        .getResponseHeaders()
        .set("Content-Type", String.format("application/json; charset=%s", StandardCharsets.UTF_8));
    exchange.sendResponseHeaders(response.code(), responseData.length);
    // if the response contains only code, we skip to write body.
    try (var os = exchange.getResponseBody()) {
      os.write(responseData);
    }
  }

  static Optional<String> parseTarget(URI uri) {
    var allPaths =
        Arrays.stream(uri.getPath().split("/"))
            .map(String::trim)
            .filter(s -> !s.isEmpty() && !s.isBlank())
            .collect(Collectors.toUnmodifiableList());
    // form: /resource/target
    if (allPaths.size() == 1) return Optional.empty();
    else if (allPaths.size() == 2) return Optional.of(allPaths.get(1));
    else throw new IllegalArgumentException("unsupported url: " + uri);
  }

  static Map<String, String> parseQueries(URI uri) {
    if (uri.getQuery() == null || uri.getQuery().isEmpty()) return Map.of();
    return Arrays.stream(uri.getQuery().split("&"))
        .filter(pair -> pair.split("=").length == 2)
        .collect(Collectors.toMap(p -> p.split("=")[0], p -> p.split("=")[1]));
  }

  /**
   * handle the get request.
   *
   * @param target the last keyword of url
   * @param queries queries from url
   * @return json object to return
   */
  Response get(Optional<String> target, Map<String, String> queries);

  /**
   * handle the post request.
   *
   * @param request the last keyword of url
   * @return json object to return
   */
  default Response post(PostRequest request) {
    return Response.NOT_FOUND;
  }

  /**
   * handle the delete request.
   *
   * @param target the last keyword of url
   * @return json object to return
   */
  default Response delete(String target, Map<String, String> queries) {
    return Response.NOT_FOUND;
  }
}
