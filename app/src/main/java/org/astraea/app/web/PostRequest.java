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
    return of((Map<String, Object>) new Gson().fromJson(json, Map.class));
  }

  static String handle(Object obj) {
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

  static PostRequest of(Map<String, Object> objs) {
    var raw =
        objs.entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> handle(e.getValue())));
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

  default <T> Optional<T> get(String key, Class<T> clz) {
    return Optional.ofNullable(raw().get(key)).map(v -> new Gson().fromJson(v, clz));
  }

  default <T> T value(String key, Class<T> clz) {
    return get(key, clz)
        .orElseThrow(() -> new NoSuchElementException("the value for " + key + " is nonexistent"));
  }

  // -----------------------------[string]-----------------------------//

  default Optional<String> get(String key) {
    return Optional.ofNullable(raw().get(key));
  }

  default String value(String key) {
    return get(key)
        .orElseThrow(() -> new NoSuchElementException("the value for " + key + " is nonexistent"));
  }

  default List<String> values(String key) {
    return values(key, String.class);
  }

  // -----------------------------[boolean]-----------------------------//
  default Optional<Boolean> getBoolean(String key) {
    return get(key).map(Boolean::parseBoolean);
  }

  // -----------------------------[numbers]-----------------------------//

  default short shortValue(String key) {
    return Short.parseShort(value(key));
  }

  default List<Short> shortValues(String key) {
    return doubleValues(key).stream()
        .map(Double::shortValue)
        .collect(Collectors.toUnmodifiableList());
  }

  default Optional<Short> getShort(String key) {
    return get(key).map(Short::parseShort);
  }

  default int intValue(String key) {
    return Integer.parseInt(value(key));
  }

  default List<Integer> intValues(String key) {
    return doubleValues(key).stream()
        .map(Double::intValue)
        .collect(Collectors.toUnmodifiableList());
  }

  default Optional<Integer> getInt(String key) {
    return get(key).map(Integer::parseInt);
  }

  default long longValue(String key) {
    return Long.parseLong(value(key));
  }

  default List<Long> longValues(String key) {
    return doubleValues(key).stream()
        .map(Double::longValue)
        .collect(Collectors.toUnmodifiableList());
  }

  default Optional<Long> getLong(String key) {
    return get(key).map(Long::parseLong);
  }

  default double doubleValue(String key) {
    return Double.parseDouble(value(key));
  }

  default List<Double> doubleValues(String key) {
    return values(key, Double.class);
  }

  default Optional<Double> getDouble(String key) {
    return get(key).map(Double::parseDouble);
  }

  // -----------------------------[others]-----------------------------//

  default <T> List<T> values(String key, Class<T> clz) {
    return new Gson()
        .fromJson(value(key), TypeToken.getParameterized(ArrayList.class, clz).getType());
  }

  default List<PostRequest> requests(String key) {
    return values(key, Map.class).stream()
        .map(PostRequest::of)
        .collect(Collectors.toUnmodifiableList());
  }
}
