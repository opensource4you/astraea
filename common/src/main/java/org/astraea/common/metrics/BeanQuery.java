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

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.astraea.common.Utils;

/**
 * MBean query class.
 *
 * <p>For the specific rule of query pattern, consider look into {@link ObjectName} Here is some
 * code example to initialize a {@link BeanQuery}
 *
 * <pre>{@code
 * // Query specific MBean from a JMX server:
 * BeanQuery.builder("java.lang")
 *       .property("type", "MemoryManager")
 *       .property("name", "CodeCacheManager")
 *       .build();
 *
 * // Query MBeans with specific property pattern from a JMX server:
 * BeanQuery.builder("java.lang")
 *       .property("type", "MemoryManager")
 *       .property("name", "*")
 *       .build();
 *
 * // Query all Mbeans from a JMX server:
 * BeanQuery.all()
 *
 * // Query all Mbeans under specific domain name from a JMX server:
 * BeanQuery.all("java.lang")
 *
 * // Query all Mbeans under specific domain name pattern from a JMX server:
 * BeanQuery.all("java.*")
 * }</pre>
 */
public class BeanQuery {

  public static Builder builder() {
    return new Builder();
  }

  /**
   * construct a {@link BeanQuery} that target all MBeans under every domain name
   *
   * @return a {@link BeanQuery} object that target all MBeans under every domain name
   */
  public static BeanQuery all() {
    return builder().usePropertyListPattern().build();
  }

  /**
   * construct a {@link BeanQuery} that target all MBeans under specific domain name
   *
   * @param domainName the domain name to query
   * @return a {@link BeanQuery} object that target all MBeans under specific domain name
   */
  public static BeanQuery all(String domainName) {
    return builder().domainName(domainName).usePropertyListPattern().build();
  }

  private final String domainName;
  private final Map<String, String> properties;
  private final ObjectName objectName;

  /**
   * Initialize a BeanQuery.
   *
   * @param domainName the target MBeans's domain name
   * @param properties the target MBeans's properties
   * @param usePropertyListPattern use property list pattern or not. If used, a ",*" or "*" string
   *     will be appended to ObjectName.
   */
  public BeanQuery(
      String domainName, Map<String, String> properties, boolean usePropertyListPattern) {
    this.domainName = Objects.requireNonNull(domainName);
    this.properties = Map.copyOf(Objects.requireNonNull(properties));
    this.objectName =
        Utils.packException(
            () -> {
              if (usePropertyListPattern) {
                var propertyList =
                    properties.entrySet().stream()
                        .map((entry -> String.format("%s=%s", entry.getKey(), entry.getValue())))
                        .collect(Collectors.joining(","));
                return ObjectName.getInstance(
                    domainName + ":" + propertyList + ((properties.isEmpty()) ? "*" : ",*"));
              }
              return ObjectName.getInstance(domainName, new Hashtable<>(this.properties));
            });
  }

  public String domainName() {
    return domainName;
  }

  public Map<String, String> properties() {
    return Map.copyOf(properties);
  }

  ObjectName objectName() {
    return this.objectName;
  }

  public static class Builder {

    private String domainName = "*";
    private final Map<String, String> properties = new HashMap<>();
    private boolean usePropertyListPattern = false;

    private Builder() {}

    /**
     * Apply new search property to the query being built.
     *
     * @param key the property key to match.
     * @param value the property value to match.
     * @see <a
     *     href="https://docs.oracle.com/javase/7/docs/api/javax/management/ObjectName.html">ObjectName</a>
     *     for how Oracle documentation describe property.
     * @return the current {@link Builder} instance with the new property applied.
     */
    public Builder property(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    /**
     * construct a {@link Builder} that target specific domainName.
     *
     * <pre>{@code
     * // A typical usage of BeanQuery#builder
     * BeanQuery myQuery = BeanQuery.builder("java.lang")
     *      .property("type", "Memory")
     *      .build();
     * }</pre>
     *
     * @param domainName the domain name to query
     * @return a {@link BeanQuery} object that target all MBeans under specific domain name
     */
    public Builder domainName(String domainName) {
      this.domainName = domainName;
      return this;
    }

    /**
     * Apply Property List Pattern to the query being built.
     *
     * <p>By default, the built query has property list pattern disabled. With Property List
     * Pattern, the query will match whose domain matches and that contains the same keys and
     * associated values, as well as possibly other keys and values.
     *
     * @see <a
     *     href="https://docs.oracle.com/javase/7/docs/api/javax/management/ObjectName.html">ObjectName</a>
     *     for explanation of property list pattern from Oracle documentation.
     * @return the current {@link Builder} instance with property list pattern applied.
     */
    public Builder usePropertyListPattern() {
      this.usePropertyListPattern = true;
      return this;
    }

    /**
     * Build a {@link BeanQuery} object based on current builder state.
     *
     * @return a {@link BeanQuery} with specific MBeans domain name & properties, based on the
     *     previous calling to {@link Builder#property(String, String)}.
     */
    public BeanQuery build() {
      return new BeanQuery(domainName, properties, usePropertyListPattern);
    }
  }

  static BeanQuery fromObjectName(ObjectName objectName) {
    return new BeanQuery(
        objectName.getDomain(),
        new HashMap<>(objectName.getKeyPropertyList()),
        objectName.isPropertyListPattern());
  }
}
