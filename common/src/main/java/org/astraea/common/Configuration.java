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
package org.astraea.common;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public record Configuration(Map<String, String> raw) {

  public static final Configuration EMPTY = new Configuration(Map.of());

  /**
   * @param key the key whose associated value is to be returned
   * @return string value. never null
   */
  public Optional<String> string(String key) {
    return Optional.ofNullable(raw.get(Objects.requireNonNull(key)));
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return optional {@link Pattern} compiled from the string associated with the key. never null
   */
  public Optional<Pattern> regexString(String key) {
    return string(key).map(Pattern::compile);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return integer value. never null
   */
  public Optional<Integer> integer(String key) {
    return string(key).map(Integer::parseInt);
  }

  public Optional<Long> longInteger(String key) {
    return string(key).map(Long::parseLong);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return duration value. If there is no key, return Optional.Empty
   */
  public Optional<Duration> duration(String key) {
    return string(key).map(Utils::toDuration);
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return DataSize value. If there is no key, return Optional.Empty
   */
  public Optional<DataSize> dataSize(String key) {
    return string(key).map(DataSize::of);
  }

  public int requireInteger(String key) {
    return integer(key).orElseThrow(() -> new NoSuchElementException(key + " is nonexistent"));
  }

  /**
   * @param key the key whose associated value is to be returned
   * @return string value. never null
   */
  public String requireString(String key) {
    return string(key).orElseThrow(() -> new NoSuchElementException(key + " is nonexistent"));
  }

  /**
   * @param prefix the string to be filtered and removed
   * @return new Configuration only contains which the key value starts with the prefix, and the
   *     prefix string and the following dot will be removed from the key
   */
  public Configuration filteredPrefixConfigs(String prefix) {
    return new Configuration(
        raw().entrySet().stream()
            .filter(k -> k.getKey().startsWith(prefix))
            .collect(
                Collectors.toMap(
                    i -> i.getKey().replaceFirst(prefix + '.', ""), Map.Entry::getValue)));
  }

  /**
   * @param key the key whose associated value is to be returned
   * @param separator to split string to multiple strings
   * @return string list. never null
   */
  public List<String> list(String key, String separator) {
    return string(key).map(s -> Arrays.asList(s.split(separator))).orElseGet(List::of);
  }
}
