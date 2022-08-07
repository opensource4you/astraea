/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.metrics;

import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.RuntimeOperationsException;

class Utility {

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
    public Object invoke(String actionName, Object[] params, String[] signature) {
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
