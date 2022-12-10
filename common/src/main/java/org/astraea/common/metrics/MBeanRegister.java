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
package org.astraea.common.metrics;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import javax.management.ObjectName;
import org.astraea.common.Utils;

/**
 * A helper class for construct & register a Dynamic MBean.
 *
 * @see <a href="https://docs.oracle.com/cd/E19206-01/816-4178/6madjde4k/index.html">Dynamic MBean
 *     Introduction</a>
 * @see <a href="https://docs.oracle.com/cd/E19206-01/816-4178/6madjde4l/index.html">Implementing a
 *     Dynamic MBean</a>
 */
public class MBeanRegister {

  public static LocalRegister local() {
    return new LocalRegister();
  }

  public static class LocalRegister {

    // TODO: At this moment, this builder support readonly attribute only.
    //  Enhance it if you need extra features.

    private LocalRegister() {}

    private String domainName;
    private final Map<String, String> properties = new Hashtable<>();
    private String description = "";
    private final List<MBeanAttributeInfo> attributeInfo = new ArrayList<>();
    private final Map<String, Supplier<?>> attributes = new ConcurrentHashMap<>();

    /**
     * The domain name of this MBean.
     *
     * @return this.
     */
    public LocalRegister setDomainName(String domainName) {
      this.domainName = domainName;
      return this;
    }

    /**
     * Put a new property of this MBean.
     *
     * @return this.
     */
    public LocalRegister addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Description of this Mbean.
     *
     * @return this.
     */
    public LocalRegister setDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Declare a new attribute of this Mbean.
     *
     * @param name the name of this attribute.
     * @param attributeClass the type of this attribute.
     * @param source where to retrieve the attribute value. Note that this {@link Supplier} must be
     *     thread-safe.
     * @return this.
     */
    public <T> LocalRegister addAttribute(
        String name, Class<T> attributeClass, Supplier<T> source) {
      Objects.requireNonNull(name);
      Objects.requireNonNull(attributeClass);
      Objects.requireNonNull(source);
      attributeInfo.add(
          new MBeanAttributeInfo(name, attributeClass.getName(), "", true, false, false));
      attributes.compute(
          name,
          (attribute, original) -> {
            if (original != null)
              throw new IllegalArgumentException(
                  "This attribute " + attribute + " already registered");
            else return source;
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
          var attribute = defensiveCopy.get(s);
          if (attribute == null)
            throw new AttributeNotFoundException("Cannot find attribute: " + s);
          else return attribute.get();
        }

        @Override
        public AttributeList getAttributes(String[] strings) {
          // Some consideration of the contract of this method
          // 1. An error in accessing specific attribute doesn't interrupt the whole read operation.
          // 2. Attribute with an error during the access, its value will be excluded from the
          // result.
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
              MBeanRegister.class.getName() + " doesn't support writable attribute");
        }

        @Override
        public AttributeList setAttributes(AttributeList attributeList) {
          throw new UnsupportedOperationException(
              MBeanRegister.class.getName() + " doesn't support writable attribute");
        }

        @Override
        public Object invoke(String s, Object[] objects, String[] strings) {
          throw new UnsupportedOperationException(
              MBeanRegister.class.getName() + " doesn't support invoke operation");
        }
      };
    }

    /** Build this Mbean, and register it to the local JVM MBean server. */
    public void register() {
      Utils.packException(
          () -> {
            var name = new ObjectName(domainName, new Hashtable<>(properties));
            var mBean = buildMBean();
            ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, name);
          });
    }
  }
}
