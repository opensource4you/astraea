package org.astraea.metrics.jmx;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;
import javax.management.*;
import javax.management.remote.*;

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
public class MBeanClient implements AutoCloseable {

  private final JMXServiceURL jmxServiceURL;
  private final JMXConnector jmxConnector;
  private final MBeanServerConnection mBeanServerConnection;
  private boolean isClosed;

  public MBeanClient(JMXServiceURL jmxServiceURL) {
    try {
      this.jmxServiceURL = Objects.requireNonNull(jmxServiceURL);
      this.jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
      this.isClosed = false;
      this.mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Fetch all attributes of target mbean.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exactly
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @return A {@link BeanObject} contain all attributes if target resolved successfully.
   */
  public BeanObject queryBean(BeanQuery beanQuery) {
    ensureConnected();
    try {
      // ask for MBeanInfo
      MBeanInfo mBeanInfo = mBeanServerConnection.getMBeanInfo(beanQuery.objectName());

      // create a list builder all available attributes name
      List<String> attributeName =
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
  public BeanObject queryBean(BeanQuery beanQuery, Collection<String> attributeNameCollection) {
    ensureConnected();
    try {

      // fetch attribute value from mbean server
      String[] attributeNameArray = attributeNameCollection.toArray(new String[0]);
      List<Attribute> attributeList =
          mBeanServerConnection.getAttributes(beanQuery.objectName(), attributeNameArray).asList();

      // collect attribute name & value into a map
      Map<String, Object> attributes = new HashMap<>();
      for (Attribute attribute : attributeList) {
        attributes.put(attribute.getName(), attribute.getValue());
      }

      // according to the javadoc of MBeanServerConnection#getAttributes, the API will
      // ignore any
      // error occurring during the fetch process (for example, attribute not exists). Below code
      // check for such condition and try to figure out what exactly the error is. put it into
      // attributes return result.
      Set<String> notResolvedAttributes =
          Arrays.stream(attributeNameArray)
              .filter(str -> !attributes.containsKey(str))
              .collect(Collectors.toSet());
      for (String attributeName : notResolvedAttributes) {
        attributes.put(attributeName, fetchAttributeObjectOrException(beanQuery, attributeName));
      }

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
      return mBeanServerConnection.getAttribute(beanQuery.objectName(), attributeName);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      return e;
    }
  }

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
  public Collection<BeanObject> queryBeans(BeanQuery beanQuery) {
    ensureConnected();
    try {
      return mBeanServerConnection.queryMBeans(beanQuery.objectName(), null).stream()
          .map(ObjectInstance::getObjectName)
          .map(BeanQuery::fromObjectName)
          .map(this::queryBean)
          .collect(Collectors.toSet());

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns the list of domains in which any MBean is currently registered.
   *
   * <p>The order of strings within the returned array is not defined.
   *
   * @return a {@link List} of domain name {@link String}
   */
  public List<String> listDomains() {
    try {
      return Arrays.asList(mBeanServerConnection.getDomains());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The JMX URL of current MBeanClient instance
   *
   * @return the origin JMX URL used to initiate MBeanClient
   */
  public JMXServiceURL getAddress() {
    return jmxServiceURL;
  }

  private void ensureConnected() {
    if (isClosed) throw new IllegalStateException("MBean client is closed");
  }

  @Override
  public void close() throws Exception {
    ensureConnected();
    this.isClosed = true;
    this.jmxConnector.close();
  }
}
