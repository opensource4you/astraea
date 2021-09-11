package org.astraea.metrics.jmx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.astraea.metrics.jmx.utils.BeanUtility;

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
}
