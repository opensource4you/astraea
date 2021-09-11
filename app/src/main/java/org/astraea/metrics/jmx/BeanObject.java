package org.astraea.metrics.jmx;

import java.util.HashMap;
import java.util.Map;

public class BeanObject {
  private final String domainName;
  private final Map<String, String> properties;
  private final Map<String, Object> attributes;

  public BeanObject(
      String domainName, Map<String, String> properties, Map<String, Object> attributes) {
    this.domainName = domainName;
    this.properties = new HashMap<>(properties); // making defensive copy
    this.attributes = new HashMap<>(attributes); // making defensive copy
  }

  public BeanObject(String domainName) {
    this.domainName = domainName;
    this.properties = new HashMap<>();
    this.attributes = new HashMap<>();
  }

  public void selectProperty(String key, String value) {
    this.properties.put(key, value);
  }

  public void fetchAttribute(String attributeName) {
    this.attributes.put(attributeName, null);
  }

  // visible for testing
  Map<String, String> getProperties() {
    return properties;
  }

  // visible for testing
  Map<String, Object> getAttributes() {
    return attributes;
  }
}
