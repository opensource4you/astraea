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
package org.astraea.app.metrics.jmx;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.common.Utils;

abstract class MBeanClientImpl implements MBeanClient {

  static MBeanClientImpl remote(JMXServiceURL jmxServiceURL) {
    try {
      var jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
      return new MBeanClientImpl(jmxConnector.getMBeanServerConnection()) {
        @Override
        public String host() {
          return jmxServiceURL.getHost();
        }

        @Override
        public int port() {
          return jmxServiceURL.getPort();
        }

        @Override
        public void close() {
          Utils.packException(jmxConnector::close);
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static MBeanClientImpl local() {
    return new MBeanClientImpl(ManagementFactory.getPlatformMBeanServer()) {
      @Override
      public String host() {
        return Utils.hostname();
      }

      @Override
      public int port() {
        return -1;
      }

      @Override
      public void close() {}
    };
  }

  private final MBeanServerConnection connection;

  MBeanClientImpl(MBeanServerConnection connection) {
    this.connection = connection;
  }

  @Override
  public BeanObject queryBean(BeanQuery beanQuery) {
    try {
      // ask for MBeanInfo
      var mBeanInfo = connection.getMBeanInfo(beanQuery.objectName());

      // create a list builder all available attributes name
      var attributeName =
          Arrays.stream(mBeanInfo.getAttributes())
              .map(MBeanFeatureInfo::getName)
              .collect(Collectors.toList());

      // query the result
      return queryBean(beanQuery, attributeName);
    } catch (ReflectionException | IntrospectionException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InstanceNotFoundException e) {
      throw new NoSuchElementException(e.getMessage());
    }
  }

  @Override
  public BeanObject queryBean(BeanQuery beanQuery, Collection<String> attributeNameCollection) {
    try {
      // fetch attribute value from mbean server
      var attributeNameArray = attributeNameCollection.toArray(new String[0]);
      var attributeList =
          connection.getAttributes(beanQuery.objectName(), attributeNameArray).asList();

      // collect attribute name & value into a map
      var attributes = new HashMap<String, Object>();
      attributeList.forEach(attribute -> attributes.put(attribute.getName(), attribute.getValue()));

      // according to the javadoc of MBeanServerConnection#getAttributes, the API will
      // ignore any
      // error occurring during the fetch process (for example, attribute not exists). Below code
      // check for such condition and try to figure out what exactly the error is. put it into
      // attributes return result.
      var notResolvedAttributes =
          Arrays.stream(attributeNameArray)
              .filter(str -> !attributes.containsKey(str))
              .collect(Collectors.toSet());
      notResolvedAttributes.forEach(
          attributeName ->
              attributes.put(
                  attributeName, fetchAttributeObjectOrException(beanQuery, attributeName)));

      // collect result, and build a new BeanObject as return result
      return new BeanObject(beanQuery.domainName(), beanQuery.properties(), attributes);

    } catch (ReflectionException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InstanceNotFoundException e) {
      throw new NoSuchElementException(e.getMessage());
    }
  }

  private Object fetchAttributeObjectOrException(BeanQuery beanQuery, String attributeName) {
    // It is possible to trigger some unexpected runtime exception during the following call.
    // For example, on my machine when I try to get attribute "BootClassPath" from
    // "java.lang:type=Runtime".
    // I will get a {@link java.lang.UnsupportedOperationException} indicates that "Boot class path
    // mechanism is not supported". Those attribute actually exists, but I cannot retrieve those
    // attribute value. Doing so I get that error.
    //
    // Instead of blinding that attribute from the library user, I decided to put the
    // exception
    // into their result.
    try {
      return connection.getAttribute(beanQuery.objectName(), attributeName);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      return e;
    }
  }

  @Override
  public Collection<BeanObject> queryBeans(BeanQuery beanQuery) {
    try {
      return connection.queryMBeans(beanQuery.objectName(), null).stream()
          .map(ObjectInstance::getObjectName)
          .map(BeanQuery::fromObjectName)
          .map(this::queryBean)
          .collect(Collectors.toSet());

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public List<String> listDomains() {
    try {
      return Arrays.asList(connection.getDomains());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
