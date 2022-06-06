package org.astraea.app.web;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

public interface PostRequest {

  static PostRequest of(HttpExchange exchange) throws IOException {
    return of(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
  }

  @SuppressWarnings("unchecked")
  static PostRequest of(String json) {
    return of(
        ((Map<String, Object>) new Gson().fromJson(json, Map.class))
            .entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> handleDouble(e.getValue()))));
  }

  static String handleDouble(Object obj) {
    // the number in GSON is always DOUBLE>
    if (obj instanceof Double) {
      var value = (double) obj;
      if (value - Math.floor(value) == 0) return String.valueOf((long) Math.floor(value));
    }
    return obj.toString();
  }

  static PostRequest of(Map<String, String> raw) {
    return () -> raw;
  }

  /** @return body represented by key-value */
  Map<String, String> raw();

  default Optional<String> get(String key) {
    return Optional.ofNullable(raw().get(key));
  }

  default String value(String key) {
    var value = raw().get(key);
    if (value == null) throw new NoSuchElementException("the value for " + key + " is nonexistent");
    return value;
  }

  default double doubleValue(String key, double defaultValue) {
    return get(key).map(Double::parseDouble).orElse(defaultValue);
  }

  default double doubleValue(String key) {
    return Double.parseDouble(value(key));
  }

  default int intValue(String key, int defaultValue) {
    return get(key).map(Integer::parseInt).orElse(defaultValue);
  }

  default int intValue(String key) {
    return Integer.parseInt(value(key));
  }

  default short shortValue(String key, short defaultValue) {
    return get(key).map(Short::parseShort).orElse(defaultValue);
  }

  default short shortValue(String key) {
    return Short.parseShort(value(key));
  }
}
