package org.astraea.web;

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

  default JsonObject process(HttpExchange exchange) {
    var method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
    var start = System.currentTimeMillis();
    JsonObject response;
    try {
      switch (method) {
        case "GET":
          return get(parseTarget(exchange.getRequestURI()), parseQueries(exchange.getRequestURI()));
        case "POST":
          return post(PostRequest.of(exchange));
        case "DELETE":
          var target = parseTarget(exchange.getRequestURI());
          if (target.isPresent()) return delete(target.get());
        default:
          return ErrorObject.for404(method + " is not supported yet");
      }
    } catch (Exception e) {
      e.printStackTrace();
      return new ErrorObject(e);
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
    JsonObject response = process(exchange);

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

  /**
   * handle the get request.
   *
   * @param target the last keyword of url
   * @param queries queries from url
   * @return json object to return
   */
  JsonObject get(Optional<String> target, Map<String, String> queries);

  /**
   * handle the post request.
   *
   * @param request the last keyword of url
   * @return json object to return
   */
  default JsonObject post(PostRequest request) {
    return ErrorObject.for404("POST is not supported yet");
  }

  /**
   * handle the delete request.
   *
   * @param target the last keyword of url
   * @return json object to return
   */
  default JsonObject delete(String target) {
    return ErrorObject.for404("DELETE is not supported yet");
  }
}
