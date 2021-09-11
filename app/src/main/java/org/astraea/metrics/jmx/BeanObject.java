package org.astraea.metrics.jmx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;

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

  public BeanObject selectProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public BeanObject fetchAttribute(String attributeName) {
    this.attributes.put(attributeName, null);
    return this;
  }

  public String jmxQueryString() {
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

  void updateAttributeValue(AttributeList attributeList) {
    for (Attribute attribute : attributeList.asList()) {
      this.attributes.put(attribute.getName(), attribute.getValue());
    }
  }

  public Map<String, String> getPropertyView() {
    return Collections.unmodifiableMap(properties);
  }

  public Map<String, Object> getAttributeView() {
    return Collections.unmodifiableMap(attributes);
  }
}
