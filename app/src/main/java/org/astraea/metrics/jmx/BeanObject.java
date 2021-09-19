package org.astraea.metrics.jmx;

import java.util.*;

/** Snapshot of remote MBean value */
public class BeanObject {
  private final String domainName;
  private final Map<String, String> properties;
  private final Map<String, Object> attributes;

  public BeanObject(
      String domainName, Map<String, String> properties, Map<String, Object> attributes) {
    this.domainName = Objects.requireNonNull(domainName);
    this.properties = Collections.unmodifiableMap(Objects.requireNonNull(properties));
    this.attributes = Collections.unmodifiableMap(Objects.requireNonNull(attributes));
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
}
