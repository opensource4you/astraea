package org.astraea.metrics.jmx;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.common.Utils;

class MBeanClientImpl implements MBeanClient {

  private final JMXServiceURL jmxServiceURL;
  private final JMXConnector jmxConnector;
  private final MBeanServerConnection mBeanServerConnection;
  private boolean isClosed;

  MBeanClientImpl(JMXServiceURL jmxServiceURL) {
    try {
      this.jmxServiceURL = Objects.requireNonNull(jmxServiceURL);
      this.jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
      this.isClosed = false;
      this.mBeanServerConnection = jmxConnector.getMBeanServerConnection();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
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

  @Override
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

  @Override
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

  @Override
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

  @Override
  public String host() {
    return jmxServiceURL.getHost();
  }

  @Override
  public int port() {
    return jmxServiceURL.getPort();
  }

  private void ensureConnected() {
    if (isClosed) throw new IllegalStateException("MBean client is closed");
  }

  @Override
  public void close() {
    if (!isClosed) {
      isClosed = true;
      Utils.packException(jmxConnector::close);
    }
  }
}
