package org.astraea.metrics.jmx;

import java.util.*;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * MBean query class.
 *
 * <p>For the specific rule of query pattern, consider look into {@link ObjectName} Here is some
 * code example to initialize a {@link BeanQuery}
 *
 * <p>Query specific MBean from a JMX server:
 *
 * <pre>{@code
 * BeanQuery.of("java.lang")
 *       .whereProperty("type", "MemoryManager")
 *       .whereProperty("name", "CodeCacheManager")
 *       .build();
 * }</pre>
 *
 * <p>Query MBeans with specific property pattern from a JMX server:
 *
 * <pre>{@code
 * BeanQuery.of("java.lang")
 *       .whereProperty("type", "MemoryManager")
 *       .whereProperty("name", "*")
 *       .build();
 * }</pre>
 *
 * <p>Query all Mbeans from a JMX server:
 *
 * <pre>{@code
 * BeanQuery.all()
 * }</pre>
 *
 * <p>Query all Mbeans under specific domain name from a JMX server:
 *
 * <pre>{@code
 * BeanQuery.all("java.lang")
 * }</pre>
 *
 * <p>Query all Mbeans under specific domain name pattern from a JMX server:
 *
 * <pre>{@code
 * BeanQuery.all("java.*")
 * }</pre>
 */
public class BeanQuery {

  private final String domainName;
  private final Map<String, String> properties;
  private final ObjectName objectName;

  /**
   * Initialize a BeanQuery. Target all mbeans under specific domain name.
   *
   * @param domainName the target MBeans's domain name
   * @throws IllegalArgumentException if any given domain name or properties is in invalid format
   */
  public BeanQuery(String domainName) throws IllegalArgumentException {
    this.domainName = Objects.requireNonNull(domainName);
    this.properties = Map.of();
    try {
      objectName = ObjectName.getInstance(domainName + ":*");
    } catch (MalformedObjectNameException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Initialize a BeanQuery.
   *
   * @param domainName the target MBeans's domain name
   * @param properties the target MBeans's properties
   * @throws IllegalArgumentException if any given domain name or properties is in invalid format
   */
  public BeanQuery(String domainName, Map<String, String> properties)
      throws IllegalArgumentException {
    this.domainName = Objects.requireNonNull(domainName);
    this.properties = Map.copyOf(Objects.requireNonNull(properties));
    Hashtable<String, String> ht = new Hashtable<>(this.properties);
    try {
      objectName = ObjectName.getInstance(domainName, ht);
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

  static class BeanQueryBuilder {

    private final String domainName;
    private final Map<String, String> properties;

    BeanQueryBuilder(String domainName) {
      this.domainName = domainName;
      this.properties = new HashMap<>();
    }

    BeanQueryBuilder(String domainName, Map<String, String> properties) {
      this.domainName = domainName;
      this.properties = new HashMap<>(properties);
    }

    public BeanQueryBuilder whereProperty(String key, String value) {
      this.properties.put(key, value);
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
      if (properties.isEmpty())
        // This call target all mbeans under specific mbeans.
        return new BeanQuery(domainName);
      else return new BeanQuery(domainName, properties);
    }
  }

  public static BeanQuery all() {
    return new BeanQueryBuilder("*").build();
  }

  public static BeanQuery all(String domainName) {
    return new BeanQueryBuilder(domainName).build();
  }

  public static BeanQueryBuilder of(String domainName) {
    return new BeanQueryBuilder(domainName);
  }

  public static BeanQueryBuilder of(String domainName, Map<String, String> properties) {
    return new BeanQueryBuilder(domainName, properties);
  }

  static BeanQuery fromObjectName(ObjectName objectName) {
    return new BeanQuery(objectName.getDomain(), new HashMap<>(objectName.getKeyPropertyList()));
  }
}
