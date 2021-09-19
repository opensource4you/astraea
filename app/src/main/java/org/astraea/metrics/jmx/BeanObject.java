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

  public Map<String, String> getPropertyView() {
    return properties;
  }

  public Map<String, Object> getAttributeView() {
    return attributes;
  }

  public Set<String> getAttributeKeySet() {
    return attributes.keySet();
  }

  public Set<String> getPropertyKeySet() {
    return properties.keySet();
  }

  public Optional<Object> getAttribute(String attributeName) {
    return Optional.ofNullable(getAttributeView().get(attributeName));
  }

  public Object getAttribute(String attributeName, Object defaultValue) {
    return getAttributeView().getOrDefault(attributeName, defaultValue);
  }

  public Object getAttribute(String attributeName, Supplier<Object> defaultValue) {
    if (getAttributeView().containsKey(attributeName)) return getAttributeView().get(attributeName);
    else return defaultValue.get();
  }

  public Optional<String> getProperty(String propertyKey) {
    return Optional.ofNullable(getPropertyView().get(propertyKey));
  }

  public String getProperty(String propertyKey, String defaultValue) {
    return getPropertyView().getOrDefault(propertyKey, defaultValue);
  }

  public Object getProperty(String propertyKey, Supplier<String> defaultValue) {
    if (getPropertyView().containsKey(propertyKey)) return getPropertyView().get(propertyKey);
    else return defaultValue.get();
  }
}
