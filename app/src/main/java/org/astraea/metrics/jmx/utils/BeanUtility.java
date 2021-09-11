package org.astraea.metrics.jmx.utils;

import java.util.Map;

public class BeanUtility {

  public static String getBeanObjectNameString(String domainName, Map<String, String> properties) {
    StringBuilder sb = new StringBuilder();

    // append domain name part
    sb.append(domainName);
    sb.append(":");

    // append properties selector part
    boolean ignoreFirstComma = true;
    for (String key : properties.keySet()) {

      if (ignoreFirstComma) ignoreFirstComma = false;
      else sb.append(",");

      sb.append(key).append("=").append(properties.get(key));
    }

    return sb.toString();
  }
}
