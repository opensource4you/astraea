package org.astraea.metrics.jmx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.ObjectName;
import org.astraea.metrics.jmx.utils.BeanUtility;

/**
 * represent an MBean object at the remote MBean server.
 *
 * <p>This class should cooperate with {@link MBeanClient} to retrieve Mbean attributes value.
 *
 * <p><strong>This class follow immutable class design</strong>, beware the internal state when
 * modify code.
 */
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

  public BeanObject(BeanObject oldObject, AttributeList list) {
    this.domainName = oldObject.domainName;
    this.properties = new HashMap<>(oldObject.properties);
    this.attributes = new HashMap<>(oldObject.attributes);
    for (Attribute attribute : list.asList()) {
      this.attributes.put(attribute.getName(), attribute.getValue());
    }
  }

  public BeanObject(ObjectName from) {
    this.domainName = from.getDomain();
    this.properties = new HashMap<>(from.getKeyPropertyList());
    this.attributes = new HashMap<>();
  }

  public BeanObject selectProperty(String key, String value) {
    HashMap<String, String> propertiesCopy = new HashMap<>(properties);
    propertiesCopy.put(key, value);
    return new BeanObject(domainName, propertiesCopy, attributes);
  }

  public BeanObject fetchAttribute(String attributeName) {
    HashMap<String, Object> attributesCopy = new HashMap<>(attributes);
    attributesCopy.put(attributeName, null);
    return new BeanObject(domainName, properties, attributesCopy);
  }

  public String objectName() {
    return BeanUtility.getBeanObjectNameString(domainName, properties);
  }

  public Map<String, String> getPropertyView() {
    return Collections.unmodifiableMap(properties);
  }

  public Map<String, Object> getAttributeView() {
    return Collections.unmodifiableMap(attributes);
  }

  public static BeanObject fromDomainName(String domainName) {
    return new BeanObject(domainName);
  }
}
