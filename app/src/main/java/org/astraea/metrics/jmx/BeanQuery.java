package org.astraea.metrics.jmx;

import java.util.*;
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
 * BeanQuery.of("java.lang")
 *       .whereProperty("type", "MemoryManager")
 *       .whereProperty("name", "CodeCacheManager")
 *       .build();
 *
 * // Query MBeans with specific property pattern from a JMX server:
 * BeanQuery.of("java.lang")
 *       .whereProperty("type", "MemoryManager")
 *       .whereProperty("name", "*")
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
public class BeanQuery implements Query {

  private final String domainName;
  private final Map<String, String> properties;
  private final ObjectName objectName;
  private final boolean usePropertyListPattern;

  /**
   * Initialize a BeanQuery.
   *
   * @param domainName the target MBeans's domain name
   * @param properties the target MBeans's properties
   * @param usePropertyListPattern use property list pattern or not. If used, a ",*" or "*" string
   *     will be appended to ObjectName.
   * @throws IllegalArgumentException if any given domain name or properties is in invalid format
   */
  public BeanQuery(
      String domainName, Map<String, String> properties, boolean usePropertyListPattern)
      throws IllegalArgumentException {
    this.domainName = Objects.requireNonNull(domainName);
    this.properties = Map.copyOf(Objects.requireNonNull(properties));
    this.usePropertyListPattern = usePropertyListPattern;
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

  @Override
  public Map<String, String> properties() {
    return Map.copyOf(properties);
  }

  @Override
  public ObjectName getQuery() {
    return this.objectName;
  }

  static class BeanQueryBuilder {

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

    public BeanQueryBuilder whereProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public BeanQueryBuilder usePropertyListPattern() {
      this.usePropertyListPattern = true;
      return this;
    }

    public BeanQueryBuilder noPropertyListPattern() {
      this.usePropertyListPattern = false;
      return this;
    }

    /**
     * Build a {@link BeanQuery} object based on current builder state.
     *
     * @return a {@link BeanQuery} with specific MBeans domain name & properties, based on the
     *     previous calling to {@link BeanQueryBuilder#whereProperty(String, String)}.
     * @throws IllegalArgumentException if domain name or any property is in invalid format.
     */
    public BeanQuery build() throws IllegalArgumentException {
      return new BeanQuery(domainName, properties, usePropertyListPattern);
    }
  }

  public static BeanQuery all() {
    return new BeanQueryBuilder("*").usePropertyListPattern().build();
  }

  public static BeanQuery all(String domainName) {
    return new BeanQueryBuilder(domainName).usePropertyListPattern().build();
  }

  public static BeanQueryBuilder of(String domainName) {
    return new BeanQueryBuilder(domainName);
  }

  public static BeanQueryBuilder of(String domainName, Map<String, String> properties) {
    return new BeanQueryBuilder(domainName, properties);
  }

  static BeanQuery fromObjectName(ObjectName objectName) {
    return new BeanQuery(
        objectName.getDomain(),
        new HashMap<>(objectName.getKeyPropertyList()),
        objectName.isPropertyListPattern());
  }
}
