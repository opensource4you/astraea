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
package org.astraea.common.metrics.jmx;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.astraea.common.Utils;

/**
 * A helper class for construct a Dynamic MBean.
 *
 * @see <a href="https://docs.oracle.com/cd/E19206-01/816-4178/6madjde4k/index.html">Dynamic MBean
 *     Introduction</a>
 * @see <a href="https://docs.oracle.com/cd/E19206-01/816-4178/6madjde4l/index.html">Implementing a
 *     Dynamic MBean</a>
 */
public class DynamicMbean {

  // TODO: At this moment, this builder support attribute read only.
  //  Enhance it if you need extra features.

  private String domainName;
  private final Map<String, String> properties = new Hashtable<>();
  private String description = "";
  private final List<MBeanAttributeInfo> attributeInfo = new ArrayList<>();
  private final Map<String, AttributeField<?>> attributes = new ConcurrentHashMap<>();

  public static DynamicMbean builder() {
    return new DynamicMbean();
  }

  public DynamicMbean domainName(String domainName) {
    this.domainName = domainName;
    return this;
  }

  public DynamicMbean property(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public DynamicMbean description(String description) {
    this.description = description;
    return this;
  }

  public <T> DynamicMbean attribute(String name, Class<T> attributeClass, Supplier<T> source) {
    attributeInfo.add(
        new MBeanAttributeInfo(name, attributeClass.getName(), "", true, false, false));
    attributes.compute(
        name,
        (attribute, original) -> {
          if (original != null)
            throw new IllegalArgumentException(
                "This attribute " + attribute + " already registered");
          else return new AttributeField<>(attributeClass, source);
        });
    return this;
  }

  private DynamicMBean buildMBean() {
    final MBeanInfo mBeanInfo =
        new MBeanInfo(
            Object.class.getName(),
            description,
            attributeInfo.toArray(MBeanAttributeInfo[]::new),
            new MBeanConstructorInfo[0],
            new MBeanOperationInfo[0],
            new MBeanNotificationInfo[0]);
    final var defensiveCopy = Map.copyOf(attributes);

    return new DynamicMBean() {
      @Override
      public MBeanInfo getMBeanInfo() {
        return mBeanInfo;
      }

      @Override
      public Object getAttribute(String s) throws AttributeNotFoundException {
        var attribute = defensiveCopy.getOrDefault(s, null);
        if (attribute == null) throw new AttributeNotFoundException("Cannot find attribute: " + s);
        else return attribute.source.get();
      }

      @Override
      public AttributeList getAttributes(String[] strings) {
        // Some consideration of the contract of this method
        // 1. An error in accessing specific attribute doesn't interrupt the whole read operation.
        // 2. Attribute with an error during the access, its value will be excluded from the result.
        var list = new AttributeList();

        for (int i = 0; i < strings.length; i++) {
          try {
            list.add(i, new Attribute(strings[i], getAttribute(strings[i])));
          } catch (Exception ignore) {
            // swallow
          }
        }

        return list;
      }

      @Override
      public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException(
            DynamicMbean.class.getName() + " doesn't support writable attribute");
      }

      @Override
      public AttributeList setAttributes(AttributeList attributeList) {
        throw new UnsupportedOperationException(
            DynamicMbean.class.getName() + " doesn't support writable attribute");
      }

      @Override
      public Object invoke(String s, Object[] objects, String[] strings) {
        throw new UnsupportedOperationException(
            DynamicMbean.class.getName() + " doesn't support invoke operation");
      }
    };
  }

  public Register build() {
    return new Register(
        Utils.packException(
            () -> new ObjectName(domainName, (Hashtable<String, String>) properties)),
        buildMBean());
  }

  public static class Register {
    private final ObjectName objectName;
    private final DynamicMBean dynamicMBean;

    private Register(ObjectName objectName, DynamicMBean dynamicMBean) {
      this.objectName = objectName;
      this.dynamicMBean = dynamicMBean;
    }

    public void register(MBeanServer server) {
      Utils.packException(() -> server.registerMBean(dynamicMBean, objectName));
    }

    public void unregister(MBeanServer server) {
      Utils.packException(() -> server.unregisterMBean(objectName));
    }
  }

  private static class AttributeField<T> {
    private final Class<T> theClass;
    private final Supplier<T> source;

    private AttributeField(Class<T> theClass, Supplier<T> source) {
      this.theClass = theClass;
      this.source = source;
    }
  }
}
