package org.astraea.metrics.jmx;

import static java.util.Map.*;

import java.util.*;
import java.util.stream.Collectors;

/** Snapshot of remote MBean value */
public class BeanObject {
  private final String domainName;
  private final Map<String, String> properties;
  private final Map<String, Object> attributes;

  /**
   * construct a {@link BeanObject}
   *
   * <p>Note that, for safety reason. Any null key/value entries may be discarded from the given
   * properties & attributes map.
   *
   * @param domainName domain name of given Mbean snapshot
   * @param properties properties of given Mbean snapshot
   * @param attributes attribute and their value of given Mbean snapshot
   */
  public BeanObject(
      String domainName, Map<String, String> properties, Map<String, Object> attributes) {
    this.domainName = Objects.requireNonNull(domainName);

    // copy properties, and remove null key or null value
    Objects.requireNonNull(properties);
    Map<String, String> propertyMap =
        properties.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.properties = Collections.unmodifiableMap(propertyMap);

    // copy attribute, and remove null key or null value
    Objects.requireNonNull(attributes);
    Map<String, Object> attributeMap =
        attributes.entrySet().stream()
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.attributes = Collections.unmodifiableMap(attributeMap);
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
