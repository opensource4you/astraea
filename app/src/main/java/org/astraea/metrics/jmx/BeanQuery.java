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
 * <pre>{@code
 * BeanQuery.of("java.lang")
 *       .whereProperty("type", "MemoryManager")
 *       .whereProperty("name", "CodeCacheManager")
 * }</pre>
 */
public class BeanQuery {

  private final String domainName;
  private final Map<String, String> properties;
  private final ObjectName objectName;

  public BeanQuery(String domainName, Map<String, String> properties) {
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

  public Map<String, String> propertyView() {
    return Map.copyOf(properties);
  }

  ObjectName objectName() {
    return this.objectName;
  }

  public BeanQuery whereProperty(String key, String value) {
    var propertiesCopy = new HashMap<>(properties);
    propertiesCopy.put(key, value);
    return new BeanQuery(domainName, propertiesCopy);
  }

  public static HalfBakedBeanQuery of(String domainName) {
    return new HalfBakedBeanQuery(domainName);
  }

  public static BeanQuery of(String domainName, Map<String, String> properties) {
    return new BeanQuery(domainName, properties);
  }

  static BeanQuery of(ObjectName objectName) {
    return new BeanQuery(objectName.getDomain(), new HashMap<>(objectName.getKeyPropertyList()));
  }

  /**
   * Represent a non-finished BeanQuery.
   *
   * <p>There is a requirement for MBean {@link ObjectName}, it requires at least one property in
   * {@link ObjectName}. Those who don't follow this rule will get a {@link
   * MalformedObjectNameException} during {@link ObjectName} initialization.
   *
   * <p>This class is here to represent those {@link BeanQuery} that have zero property. Since this
   * class has no {@link BeanQuery#objectName()}, and no relationship with {@link BeanQuery}. User
   * cannot take this half-baked BeanQuery object to retrieve data from {@link MBeanClient}. Prevent
   * such error from compile-time.
   */
  public static class HalfBakedBeanQuery {

    private final String domainName;

    private HalfBakedBeanQuery(String domainName) {
      this.domainName = domainName;
    }

    public BeanQuery whereProperty(String key, String value) {
      return new BeanQuery(domainName, Map.of(key, value));
    }
  }
}
