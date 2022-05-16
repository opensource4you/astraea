package org.astraea.web;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

public interface PostRequest {

  @SuppressWarnings("unchecked")
  static PostRequest of(HttpExchange exchange) throws IOException {
    return of(
        ((Map<String, Object>)
                new Gson()
                    .fromJson(
                        new String(
                            exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
                        Map.class))
            .entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
  }

  static PostRequest of(Map<String, String> raw) {
    return () -> raw;
  }

  /** @return body represented by key-value */
  Map<String, String> raw();

  default String value(String key) {
    var value = raw().get(key);
    if (value == null) throw new NoSuchElementException("the value for " + key + " is nonexistent");
    return value;
  }

  default double doubleValue(String key, double defaultValue) {
    try {
      // the number in GSON is always DOUBLE>
      return Optional.ofNullable(raw().get(key)).map(Double::parseDouble).orElse(defaultValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  default double doubleValue(String key) {
    try {
      return Double.parseDouble(value(key));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  default int intValue(String key, int defaultValue) {
    return (int) doubleValue(key, defaultValue);
  }

  default int intValue(String key) {
    return (int) doubleValue(key);
  }

  default short shortValue(String key, short defaultValue) {
    return (short) doubleValue(key, defaultValue);
  }

  default short shortValue(String key) {
    return (short) doubleValue(key);
  }
}
