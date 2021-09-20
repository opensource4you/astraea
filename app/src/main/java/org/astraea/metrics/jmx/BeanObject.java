package org.astraea.metrics.jmx;

import java.util.*;
import java.util.stream.Collectors;

/** Snapshot of remote MBean value */
public class BeanObject {
  private final String domainName;
  private final Map<String, String> properties;
  private final Map<String, Object> attributes;

  public BeanObject(
      String domainName, Map<String, String> properties, Map<String, Object> attributes) {
    this.domainName = Objects.requireNonNull(domainName);
    this.properties = Map.copyOf(Objects.requireNonNull(properties));
    // This is impossible to use Map#copyOf for following statement, since Map#copyOf will check
    // if any key/value is null. It's possible that the attribute value returned from JMX Server is
    // null. If we use Map#copyOf here we will get unexpected error for some MBeans result.
    //noinspection Java9CollectionFactory
    this.attributes =
        Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(attributes)));
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

  @Override
  public String toString() {
    String propertyList =
        properties.entrySet().stream()
            .map((entry -> entry.getKey() + "=" + entry.getValue()))
            .collect(Collectors.joining(","));
    return "[" + domainName + ":" + propertyList + "]\n" + attributes;
  }
}
