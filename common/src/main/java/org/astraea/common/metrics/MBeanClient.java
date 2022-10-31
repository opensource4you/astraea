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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.common.Utils;

/**
 * A MBeanClient used to retrieve mbean value from remote Jmx server.
 *
 * <pre>{@code
 * try(MBeanClient client = new MBeanClient(jmxConnectorServer.getAddress())) {
 *   BeanObject bean = client.queryBean(BeanQuery.builder("java.lang")
 *            .property("type", "MemoryManager")
 *            .property("name", "CodeCacheManager")
 *            .build());
 *   System.out.println(bean.getAttributes());
 * }</pre>
 */
public interface MBeanClient extends AutoCloseable {

  /**
   * @param host the address of jmx server
   * @param port the port of jmx server
   * @return a mbean client using JNDI to lookup metrics.
   */
  static MBeanClient jndi(String host, int port) {
    try {
      return of(
          new JMXServiceURL(
              String.format(
                  "service:jmx:rmi://%s:%s/jndi/rmi://%s:%s/jmxrmi", host, port, host, port)));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  static MBeanClient of(JMXServiceURL jmxServiceURL) {
    try {
      var jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
      return new AbstractMBeanClient(jmxConnector.getMBeanServerConnection()) {
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

  static MBeanClient local() {
    return new AbstractMBeanClient(ManagementFactory.getPlatformMBeanServer()) {
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

  /**
   * Fetch all attributes of target mbean.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @return A {@link BeanObject} contain all attributes if target resolved successfully.
   */
  BeanObject queryBean(BeanQuery beanQuery);

  /**
   * Fetch given attributes of target mbean
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @param attributeNameCollection a list of attribute you want to retrieve
   * @return A {@link BeanObject} contain given specific attributes if target resolved successfully.
   */
  BeanObject queryBean(BeanQuery beanQuery, Collection<String> attributeNameCollection);

  /**
   * Query mBeans by pattern.
   *
   * <p>Query mbeans by {@link ObjectName} pattern, the returned {@link BeanObject}s will contain
   * all the available attributes
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the pattern to query
   * @return A {@link Set} of {@link BeanObject}, all BeanObject has its own attributes resolved.
   */
  Collection<BeanObject> queryBeans(BeanQuery beanQuery);

  /**
   * Returns the list of domains in which any MBean is currently registered.
   *
   * <p>The order of strings within the returned array is not defined.
   *
   * @return a {@link List} of domain name {@link String}
   */
  List<String> listDomains();

  /**
   * @return the host address of jmx server
   */
  String host();

  /**
   * @return the port listened by jmx server
   */
  int port();

  @Override
  void close();

  abstract class AbstractMBeanClient implements MBeanClient {

    private final MBeanServerConnection connection;

    AbstractMBeanClient(MBeanServerConnection connection) {
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
        attributeList.forEach(
            attribute -> attributes.put(attribute.getName(), attribute.getValue()));

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
      // I will get a {@link java.lang.UnsupportedOperationException} indicates that "Boot class
      // path
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
}
