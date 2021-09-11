package org.astraea.metrics.jmx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    StringBuilder sb = new StringBuilder();

    // append domain name part
    sb.append(domainName);
    sb.append(":");

    // append properties selector part
    boolean ignoreFirstComma = true;
    for (String key : propertyQuery.keySet()) {

      if (ignoreFirstComma) ignoreFirstComma = false;
      else sb.append(",");

      sb.append(key).append("=").append(propertyQuery.get(key));
    }

    return sb.toString();
  }

  public static BeanQuery forDomainName(String domainName) {
    return new BeanQuery(domainName);
  }
}
