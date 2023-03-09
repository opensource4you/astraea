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
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.common.Utils;

/**
 * A MBeanClient used to retrieve mbean value from remote Jmx server.
 *
 * <pre>{@code
 * try(var client = new MBeanClient(jmxConnectorServer.getAddress())) {
 *   var bean = client.bean(BeanQuery.builder("java.lang")
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
    return Utils.packException(
        () -> {
          var jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
          return new AbstractMBeanClient(
              jmxConnector.getMBeanServerConnection(),
              jmxServiceURL.getHost(),
              jmxServiceURL.getPort()) {
            @Override
            public void close() {
              Utils.packException(jmxConnector::close);
            }
          };
        });
  }

  static MBeanClient local() {
    return new AbstractMBeanClient(
        ManagementFactory.getPlatformMBeanServer(), Utils.hostname(), -1) {
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
  BeanObject bean(BeanQuery beanQuery);

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
  Collection<BeanObject> beans(BeanQuery beanQuery);

  @Override
  void close();

  abstract class AbstractMBeanClient implements MBeanClient {

    private final MBeanServerConnection connection;
    final String host;

    final int port;

    AbstractMBeanClient(MBeanServerConnection connection, String host, int port) {
      this.connection = connection;
      this.host = host;
      this.port = port;
    }

    @Override
    public BeanObject bean(BeanQuery beanQuery) {
      return Utils.packException(
          () -> {
            // ask for MBeanInfo
            var mBeanInfo = connection.getMBeanInfo(beanQuery.objectName());

            // create a list builder all available attributes name
            var attributeName =
                Arrays.stream(mBeanInfo.getAttributes())
                    .map(MBeanFeatureInfo::getName)
                    .collect(Collectors.toList());

            // query the result
            return queryBean(beanQuery, attributeName);
          });
    }

    BeanObject queryBean(BeanQuery beanQuery, Collection<String> attributeNameCollection) {
      return Utils.packException(
          () -> {
            // fetch attribute value from mbean server
            var attributeNameArray = attributeNameCollection.toArray(new String[0]);
            var attributeList =
                connection.getAttributes(beanQuery.objectName(), attributeNameArray).asList();

            // collect attribute name & value into a map
            var attributes = new HashMap<String, Object>();
            attributeList.forEach(
                attribute -> attributes.put(attribute.getName(), attribute.getValue()));

            // according to the javadoc of MBeanServerConnection#getAttributes, the API will
            // ignore any error occurring during the fetch process (for example, attribute not
            // exists). Below code check for such condition and try to figure out what exactly
            // the error is. put it into attributes return result.
            Arrays.stream(attributeNameArray)
                .filter(str -> !attributes.containsKey(str))
                .distinct()
                .forEach(
                    attributeName -> {
                      try {
                        var r = connection.getAttribute(beanQuery.objectName(), attributeName);
                        attributes.put(attributeName, r);
                      } catch (RuntimeMBeanException e) {
                        if (!(e.getCause() instanceof UnsupportedOperationException))
                          throw new IllegalStateException(e);
                        // the UnsupportedOperationException is thrown when we query unacceptable
                        // attribute. we just skip it as it is normal case to
                        // return "acceptable" attribute only
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    });

            // collect result, and build a new BeanObject as return result
            return new BeanObject(beanQuery.domainName(), beanQuery.properties(), attributes);
          });
    }

    @Override
    public Collection<BeanObject> beans(BeanQuery beanQuery) {
      return Utils.packException(
          () ->
              connection.queryMBeans(beanQuery.objectName(), null).stream()
                  // Parallelize the sampling of bean objects. The underlying RMI is thread-safe.
                  // https://github.com/skiptests/astraea/issues/1553#issuecomment-1461143723
                  .parallel()
                  .map(ObjectInstance::getObjectName)
                  .map(BeanQuery::fromObjectName)
                  .map(this::bean)
                  .collect(Collectors.toSet()));
    }

    /**
     * Returns the list of domains in which any MBean is currently registered.
     *
     * <p>The order of strings within the returned array is not defined.
     *
     * @return a {@link List} of domain name {@link String}
     */
    List<String> domains() {
      return Utils.packException(() -> Arrays.asList(connection.getDomains()));
    }
  }
}
