package org.astraea.app.metrics.jmx;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

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
    try {
      if (usePropertyListPattern) {
        String propertyList =
            properties.entrySet().stream()
                .map((entry -> String.format("%s=%s", entry.getKey(), entry.getValue())))
                .collect(Collectors.joining(","));
        StringBuilder sb = new StringBuilder();
        sb.append(domainName);
        sb.append(":");
        sb.append(propertyList);
        sb.append((properties.size() > 0) ? ",*" : "*");
        this.objectName = ObjectName.getInstance(sb.toString());
      } else {
        Hashtable<String, String> ht = new Hashtable<>(this.properties);
        this.objectName = ObjectName.getInstance(domainName, ht);
      }
    } catch (MalformedObjectNameException e) {
      throw new IllegalArgumentException(e);
    }
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

  public static class BeanQueryBuilder {

    private final String domainName;
    private final Map<String, String> properties;
    private boolean usePropertyListPattern;

    BeanQueryBuilder(String domainName) {
      this.domainName = domainName;
      this.properties = new HashMap<>();
      this.usePropertyListPattern = false;
    }

    BeanQueryBuilder(String domainName, Map<String, String> properties) {
      this.domainName = domainName;
      this.properties = new HashMap<>(properties);
      this.usePropertyListPattern = false;
    }

    /**
     * Apply new search property to the query being built.
     *
     * @param key the property key to match.
     * @param value the property value to match.
     * @see <a
     *     href="https://docs.oracle.com/javase/7/docs/api/javax/management/ObjectName.html">ObjectName</a>
     *     for how Oracle documentation describe property.
     * @return the current {@link BeanQueryBuilder} instance with the new property applied.
     */
    public BeanQueryBuilder property(String key, String value) {
      this.properties.put(key, value);
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
     * @return the current {@link BeanQueryBuilder} instance with property list pattern applied.
     */
    public BeanQueryBuilder usePropertyListPattern() {
      this.usePropertyListPattern = true;
      return this;
    }

    /**
     * Build a {@link BeanQuery} object based on current builder state.
     *
     * @return a {@link BeanQuery} with specific MBeans domain name & properties, based on the
     *     previous calling to {@link BeanQueryBuilder#property(String, String)}.
     */
    public BeanQuery build() {
      return new BeanQuery(domainName, properties, usePropertyListPattern);
    }
  }

  /**
   * construct a {@link BeanQuery} that target all MBeans under every domain name
   *
   * @return a {@link BeanQuery} object that target all MBeans under every domain name
   */
  public static BeanQuery all() {
    return new BeanQueryBuilder("*").usePropertyListPattern().build();
  }

  /**
   * construct a {@link BeanQuery} that target all MBeans under specific domain name
   *
   * @param domainName the domain name to query
   * @return a {@link BeanQuery} object that target all MBeans under specific domain name
   */
  public static BeanQuery all(String domainName) {
    return new BeanQueryBuilder(domainName).usePropertyListPattern().build();
  }

  /**
   * construct a {@link BeanQueryBuilder} that target specific domainName.
   *
   * <pre>{@code
   * // A typical usage of BeanQuery#builder
   * BeanQuery myQuery = BeanQuery.builder("java.lang")
   *      .property("type", "Memory")
   *      .build();
   * }</pre>
   *
   * @param domainName the query target domain name for Builder
   * @return a {@link BeanQueryBuilder} that can be used to construct a query against specific MBean
   *     domain name
   */
  public static BeanQueryBuilder builder(String domainName) {
    return new BeanQueryBuilder(domainName);
  }

  /**
   * construct a {@link BeanQueryBuilder} that target specific domainName. With some properties
   * given already.
   *
   * @param domainName the query target domain name for Builder
   * @param properties a {@link Map} of property entries, used to initialize the builder
   * @return a {@link BeanQueryBuilder} that can be used to construct a query against specific MBean
   *     domain name
   */
  public static BeanQueryBuilder builder(String domainName, Map<String, String> properties) {
    return new BeanQueryBuilder(domainName, properties);
  }

  static BeanQuery fromObjectName(ObjectName objectName) {
    return new BeanQuery(
        objectName.getDomain(),
        new HashMap<>(objectName.getKeyPropertyList()),
        objectName.isPropertyListPattern());
  }
}
