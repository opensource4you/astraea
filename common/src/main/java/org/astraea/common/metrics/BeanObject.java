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
package org.astraea.common.metrics;

import static java.util.Map.Entry;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Snapshot of remote MBean value */
public record BeanObject(
    String domainName,
    Map<String, String> properties,
    Map<String, Object> attributes,
    long createdTimestamp) {

  /**
   * construct a {@link BeanObject}
   *
   * <p>Note that, for safety reason. Any null key/value entries may be discarded from the given
   * properties & attributes map.
   *
   * @param domainName domain name of given Mbean snapshot
   * @param properties properties of given Mbean snapshot
   * @param attributes attribute and their value of given Mbean snapshot
   */
  public BeanObject(
      String domainName, Map<String, String> properties, Map<String, Object> attributes) {
    this(domainName, properties, attributes, System.currentTimeMillis());
  }

  public BeanObject(
      String domainName,
      Map<String, String> properties,
      Map<String, Object> attributes,
      long createdTimestamp) {
    this.domainName = Objects.requireNonNull(domainName);
    // copy properties, and remove null key or null value
    this.properties =
        Objects.requireNonNull(properties).entrySet().stream()
            .filter(entry1 -> entry1.getKey() != null && entry1.getValue() != null)
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));

    // copy attribute, and remove null key or null value
    this.attributes =
        Objects.requireNonNull(attributes).entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue));
    this.createdTimestamp = createdTimestamp;
  }
}
