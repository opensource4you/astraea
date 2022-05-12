package org.astraea.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

interface Handler extends HttpHandler {

  default JsonObject response(HttpExchange exchange) {
    try {
      return response(
          parseTarget(exchange.getRequestURI()), parseQueries(exchange.getRequestURI()));
    } catch (Exception e) {
      e.printStackTrace();
      return new ErrorObject(e);
    }
  }

  @Override
  default void handle(HttpExchange exchange) throws IOException {
    var response = response(exchange);
    var responseData = response.json().getBytes(StandardCharsets.UTF_8);
    exchange
        .getResponseHeaders()
        .set("Content-Type", String.format("application/json; charset=%s", StandardCharsets.UTF_8));
    exchange.sendResponseHeaders(
        response instanceof ErrorObject ? ((ErrorObject) response).code : 200, responseData.length);
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

  JsonObject response(Optional<String> target, Map<String, String> queries);
}
