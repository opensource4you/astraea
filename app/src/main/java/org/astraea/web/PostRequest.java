package org.astraea.web;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
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

  default String value(String key) {
    var value = raw().get(key);
    if (value == null) throw new NoSuchElementException("the value for " + key + " is nonexistent");
    return value;
  }

  default double doubleValue(String key) {
    try {
      return Double.parseDouble(value(key));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  default int intValue(String key) {
    // the number in GSON is always DOUBLE>
    return (int) doubleValue(key);
  }
}
