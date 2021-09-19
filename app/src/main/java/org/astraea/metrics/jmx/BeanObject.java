package org.astraea.metrics.jmx;

import java.util.*;
import java.util.function.Supplier;

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

  public Set<String> getAttributeKeySet() {
    return attributes.keySet();
  }

  public Set<String> getPropertyKeySet() {
    return properties.keySet();
  }

  public Optional<Object> getAttribute(String attributeName) {
    return Optional.ofNullable(getAttributes().get(attributeName));
  }

  public Object getAttribute(String attributeName, Object defaultValue) {
    return getAttributes().getOrDefault(attributeName, defaultValue);
  }

  public Object getAttribute(String attributeName, Supplier<Object> defaultValue) {
    if (getAttributes().containsKey(attributeName)) return getAttributes().get(attributeName);
    else return defaultValue.get();
  }

  public Optional<String> getProperty(String propertyKey) {
    return Optional.ofNullable(getProperties().get(propertyKey));
  }

  public String getProperty(String propertyKey, String defaultValue) {
    return getProperties().getOrDefault(propertyKey, defaultValue);
  }

  public Object getProperty(String propertyKey, Supplier<String> defaultValue) {
    if (getProperties().containsKey(propertyKey)) return getProperties().get(propertyKey);
    else return defaultValue.get();
  }
}
