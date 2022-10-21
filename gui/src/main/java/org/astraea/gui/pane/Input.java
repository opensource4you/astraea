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
package org.astraea.gui.pane;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface Input {
  default <T> T singleSelectedRadio(T defaultObj) {
    return multiSelectedRadios(List.of(defaultObj)).get(0);
  }

  <T> List<T> multiSelectedRadios(List<T> defaultObjs);

  /** @return the keys having empty/blank value. */
  default Set<String> emptyValueKeys() {
    return texts().entrySet().stream()
        .filter(entry -> entry.getValue().isEmpty())
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  /** @return the input key and value. The value is not empty. */
  default Map<String, String> nonEmptyTexts() {
    return texts().entrySet().stream()
        .filter(entry -> entry.getValue().isPresent())
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().get()));
  }

  /** @return the input key and value. The value could be empty. */
  Map<String, Optional<String>> texts();

  boolean matchSearch(String word);
}
