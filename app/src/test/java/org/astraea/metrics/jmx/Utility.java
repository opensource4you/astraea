package org.astraea.metrics.jmx;

import java.util.Map;
import javax.management.*;

public class Utility {

  static class DynamicMBean implements javax.management.DynamicMBean {

    private final MBeanInfo mBeanInfo;
    private final Map<String, ?> readonlyAttributes;

    DynamicMBean(Map<String, ?> readonlyAttributes) {
      this.readonlyAttributes = readonlyAttributes;
      MBeanAttributeInfo[] attributeInfos =
          readonlyAttributes.entrySet().stream()
              .map(
                  entry ->
                      new MBeanAttributeInfo(
                          entry.getKey(),
                          entry.getValue().getClass().getName(),
                          "",
                          true,
                          false,
                          false))
              .toArray(MBeanAttributeInfo[]::new);
      this.mBeanInfo =
          new MBeanInfo(
              DynamicMBean.class.getName(),
              "Readonly Dynamic MBean for Testing Purpose",
              attributeInfos,
              new MBeanConstructorInfo[0],
              new MBeanOperationInfo[0],
              new MBeanNotificationInfo[0]);
    }

    @Override
    public Object getAttribute(String attributeName) throws AttributeNotFoundException {
      if (readonlyAttributes.containsKey(attributeName))
        return readonlyAttributes.get(attributeName);

      throw new AttributeNotFoundException();
    }

    @Override
    public void setAttribute(Attribute attribute) {
      throw new RuntimeOperationsException(new IllegalArgumentException("readonly"));
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
      final AttributeList list = new AttributeList();
      for (String attribute : attributes) {
        if (readonlyAttributes.containsKey(attribute))
          list.add(new Attribute(attribute, readonlyAttributes.get(attribute)));
      }

      return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
      throw new RuntimeOperationsException(new IllegalArgumentException("readonly"));
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature)
        throws MBeanException, ReflectionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public MBeanInfo getMBeanInfo() {
      return mBeanInfo;
    }
  }

  /**
   * Create a readonly dynamic MBeans
   *
   * @param attributes for each key/value pair. the key string represent the attribute name, and the
   *     value represent the attribute value
   */
  public static DynamicMBean createReadOnlyDynamicMBean(Map<String, ?> attributes) {
    return new DynamicMBean(attributes);
  }
}
