package org.astraea.metrics.jmx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.astraea.metrics.jmx.utils.BeanUtility;

/**
 * represent an MBean query string.
 *
 * <p>This class should cooperate with {@link MBeanClient} to query possible Mbean candidate.
 *
 * <p><strong>This class follow immutable class design</strong>, beware the internal state when
 * modify code.
 */
public class BeanQuery {
  private final String domainName;
  private final Map<String, String> propertyQuery;

  public BeanQuery(String domainName) {
    this.domainName = domainName;
    this.propertyQuery = Collections.emptyMap();
  }

  public BeanQuery(String domainName, Map<String, String> propertyQuery) {
    this.domainName = domainName;
    this.propertyQuery = propertyQuery;
  }

  public BeanQuery whereProperty(String key, String value) {
    Map<String, String> map = new HashMap<>(this.propertyQuery);
    map.put(key, value);

    return new BeanQuery(domainName, map);
  }

  public String queryString() {
    return BeanUtility.getBeanObjectNameString(domainName, propertyQuery);
  }

  public static BeanQuery forDomainName(String domainName) {
    return new BeanQuery(domainName);
  }

  @Override
  public String toString() {
    return "BeanQuery{"
        + "domainName='"
        + domainName
        + '\''
        + ", propertyQuery="
        + propertyQuery
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BeanQuery beanQuery = (BeanQuery) o;
    return domainName.equals(beanQuery.domainName) && propertyQuery.equals(beanQuery.propertyQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domainName, propertyQuery);
  }
}
