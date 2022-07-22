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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    // the number in GSON is always DOUBLE
    if (obj instanceof Double) {
      var value = (double) obj;
      if (value - Math.floor(value) == 0) return String.valueOf((long) Math.floor(value));
    }
    // TODO: handle double in nested type
    // use gson instead of obj.toString in nested type since obj.toString won't add double quote to
    // string and will create invalid json
    if (obj instanceof Map || obj instanceof List) {
      return new GsonBuilder().disableHtmlEscaping().create().toJson(obj);
    }
    return obj.toString();
  }

  static PostRequest of(Map<String, String> raw) {
    return () -> raw;
  }

  /** @return body represented by key-value */
  Map<String, String> raw();

  /**
   * @param keys to check
   * @return true if all keys has an associated value.
   */
  default boolean has(String... keys) {
    return Arrays.stream(keys).allMatch(raw()::containsKey);
  }

  default Optional<String> get(String key) {
    return Optional.ofNullable(raw().get(key));
  }

  default <T> List<T> values(String key, Class<T> clz) {
    return new Gson()
        .fromJson(value(key), TypeToken.getParameterized(ArrayList.class, clz).getType());
  }

  default <T> T value(String key, Class<T> clz) {
    return new Gson().fromJson(value(key), TypeToken.get(clz).getType());
  }

  default String value(String key) {
    var value = raw().get(key);
    if (value == null) throw new NoSuchElementException("the value for " + key + " is nonexistent");
    return value;
  }

  /**
   * parse the value as a string array
   *
   * @param key to search value
   * @return string array
   */
  default List<String> values(String key) {
    return new Gson().fromJson(value(key), new TypeToken<ArrayList<String>>() {}.getType());
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

  default List<Integer> ints(String key) {
    return values(key).stream()
        // the number in GSON is always DOUBLE
        .map(Double::valueOf)
        .map(Double::intValue)
        .collect(Collectors.toUnmodifiableList());
  }

  default short shortValue(String key, short defaultValue) {
    return get(key).map(Short::parseShort).orElse(defaultValue);
  }

  default short shortValue(String key) {
    return Short.parseShort(value(key));
  }

  default boolean booleanValue(String key, boolean defaultValue) {
    return get(key).map(Boolean::parseBoolean).orElse(defaultValue);
  }

  default boolean booleanValue(String key) {
    return Boolean.parseBoolean(value(key));
  }
}
