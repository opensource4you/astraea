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
package org.astraea.common.cost;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Configuration {

  static Configuration of(Map<String, String> configs) {
    return new Configuration() {
      @Override
      public Optional<String> string(String key) {
        return Optional.ofNullable(configs.get(key)).map(Object::toString);
      }

      @Override
      public List<String> list(String key, String separator) {
        return Arrays.asList(requireString(key).split(separator));
      }

      @Override
      public <K, V> Map<K, V> map(
          String key,
          String listSeparator,
          String mapSeparator,
          Function<String, K> keyConverter,
          Function<String, V> valueConverter) {
        Function<String, Map.Entry<K, V>> split =
            s -> {
              var items = s.split(mapSeparator);
              if (items.length != 2)
                throw new IllegalArgumentException(
                    "the value: " + s + " is using incorrect separator");
              return Map.entry(keyConverter.apply(items[0]), valueConverter.apply(items[1]));
            };
        return list(key, listSeparator).stream()
            .map(split)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Set<Map.Entry<String, String>> entrySet() {
        return configs.entrySet();
      }
    };
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return string value. never null
   */
  Optional<String> string(String key);

  /**
   * @param key the key whose associated value is to be returned
   * @return integer value. never null
   */
  default Optional<Integer> integer(String key) {
    return string(key).map(Integer::parseInt);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return string value. never null
   */
  default String requireString(String key) {
    return string(key).orElseThrow(() -> new NoSuchElementException(key + " is nonexistent"));
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param separator to split string to multiple strings
   * @return string list. never null
   */
  List<String> list(String key, String separator);

  /**
   * parse the map structure from value associated to specify key
   *
   * @param key the key whose associated value is to be returned
   * @param listSeparator to split string to multiple strings
   * @param mapSeparator to split multiple strings to map
   * @return map. never null
   */
  default Map<String, String> map(String key, String listSeparator, String mapSeparator) {
    return map(key, listSeparator, mapSeparator, s -> s);
  }

  /**
   * parse the map structure from value associated to specify key
   *
   * @param key the key whose associated value is to be returned
   * @param listSeparator to split string to multiple strings
   * @param mapSeparator to split multiple strings to map
   * @param valueConverter used to convert value string to specify type
   * @return map. never null
   */
  default <T> Map<String, T> map(
      String key, String listSeparator, String mapSeparator, Function<String, T> valueConverter) {
    return map(key, listSeparator, mapSeparator, s -> s, valueConverter);
  }

  /**
   * parse the map structure from value associated to specify key
   *
   * @param key the key whose associated value is to be returned
   * @param listSeparator to split string to multiple strings
   * @param mapSeparator to split multiple strings to map
   * @param keyConverter used to convert key string to specify type
   * @param valueConverter used to convert value string to specify type
   * @return map. never null
   */
  <K, V> Map<K, V> map(
      String key,
      String listSeparator,
      String mapSeparator,
      Function<String, K> keyConverter,
      Function<String, V> valueConverter);

  /**
   * @return a {@link Set} view of the mappings contained in this map.
   */
  Set<Map.Entry<String, String>> entrySet();
}
