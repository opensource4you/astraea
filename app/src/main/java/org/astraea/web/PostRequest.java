package org.astraea.web;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public interface PostRequest {

  @SuppressWarnings("unchecked")
  static PostRequest of(HttpExchange exchange) throws IOException {
    var jsonString = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    var raw =
        ((Map<String, Object>) new Gson().fromJson(jsonString, Map.class))
            .entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    return () -> raw;
  }

  /** @return body represented by key-value */
  Map<String, String> raw();
}
