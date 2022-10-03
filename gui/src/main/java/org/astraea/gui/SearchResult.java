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
package org.astraea.gui;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface SearchResult<T> {

  SearchResult<Object> EMPTY = of(List.of(), null);

  @SuppressWarnings("unchecked")
  static <T> SearchResult<T> empty() {
    return (SearchResult<T>) EMPTY;
  }

  static <R extends Map<String, String>> SearchResult<Object> of(List<R> items) {
    return of(items, null);
  }

  static <R extends Map<String, String>, T> SearchResult<T> of(List<R> items, T obj) {
    return new SearchResult<T>() {
      @Override
      public T object() {
        return obj;
      }

      @Override
      public List<Map<String, String>> items() {
        return items.stream()
            .map(item -> (Map<String, String>) item)
            .collect(Collectors.toUnmodifiableList());
      }
    };
  }

  T object();

  default Set<String> keys() {
    return items().stream()
        .flatMap(i -> i.keySet().stream())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  List<Map<String, String>> items();
}
