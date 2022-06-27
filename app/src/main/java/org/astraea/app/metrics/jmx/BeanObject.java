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
package org.astraea.app.metrics.jmx;

import static java.util.Map.Entry;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Snapshot of remote MBean value */
public class BeanObject {
  private final String domainName;
  private final Map<String, String> properties;
  private final Map<String, Object> attributes;
  private final long createdTimestamp;

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
    this.domainName = Objects.requireNonNull(domainName);

    // copy properties, and remove null key or null value
    Objects.requireNonNull(properties);
    Map<String, String> propertyMap =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.properties = Collections.unmodifiableMap(propertyMap);

    // copy attribute, and remove null key or null value
    Objects.requireNonNull(attributes);
    Map<String, Object> attributeMap =
        attributes.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.attributes = Collections.unmodifiableMap(attributeMap);
    this.createdTimestamp = System.currentTimeMillis();
  }

  public BeanObject(
      String domainName,
      Map<String, String> properties,
      Map<String, Object> attributes,
      long createdTimestamp) {
    this.domainName = Objects.requireNonNull(domainName);

    // copy properties, and remove null key or null value
    Objects.requireNonNull(properties);
    Map<String, String> propertyMap =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.properties = Collections.unmodifiableMap(propertyMap);

    // copy attribute, and remove null key or null value
    Objects.requireNonNull(attributes);
    Map<String, Object> attributeMap =
        attributes.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.attributes = Collections.unmodifiableMap(attributeMap);
    this.createdTimestamp = createdTimestamp;
  }

  public String domainName() {
    return domainName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Map<String, Object> getAttributes() {
    return attributes;
  }

  public long createdTimestamp() {
    return createdTimestamp;
  }

  @Override
  public String toString() {
    String propertyList =
        properties.entrySet().stream()
            .map((entry -> entry.getKey() + "=" + entry.getValue()))
            .collect(Collectors.joining(","));
    return "[" + domainName + ":" + propertyList + "]\n" + attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BeanObject that = (BeanObject) o;
    return domainName.equals(that.domainName)
        && properties.equals(that.properties)
        && attributes.equals(that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domainName, properties, attributes);
  }
}
