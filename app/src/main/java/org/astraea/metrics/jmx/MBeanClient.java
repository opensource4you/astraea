package org.astraea.metrics.jmx;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class MBeanClient implements AutoCloseable {

  private final JMXServiceURL jmxServiceURL;
  private JMXConnector jmxConnector;
  private MBeanServerConnection mBeanServerConnection;
  private boolean isClientConnected = false;

  public MBeanClient(String jmxUrl) throws MalformedURLException {
    this(new JMXServiceURL(jmxUrl));
  }

  public MBeanClient(JMXServiceURL jmxUrl) {
    this.jmxServiceURL = jmxUrl;
  }

  public BeanObject fetchObjectAttribute(BeanObject beanObject) {
    ensureClientConnected();

    try {
      String objectName = beanObject.objectName();
      String[] attributeNameArray = beanObject.getAttributeView().keySet().toArray(new String[0]);

      // fetch attribute list, create a new object based on these new attribute to keep everything
      // immutable.
      AttributeList attributeList =
          getAttributeList(ObjectName.getInstance(objectName), attributeNameArray);
      return new BeanObject(beanObject, attributeList);

    } catch (MalformedObjectNameException e) {
      // From the point we managed the BeanObject for library-user.
      // It is our responsibility to keep the object name string error-free, so the error shouldn't
      // be here.
      // If it actually happened, that means something wrong with the object name generation code.
      throw new IllegalStateException("Illegal object name detected, this shouldn't happens", e);
    }
  }

  public Set<BeanObject> queryObject(BeanQuery beanQuery) {
    try {

      Set<ObjectInstance> objectInstances =
          mBeanServerConnection.queryMBeans(ObjectName.getInstance(beanQuery.queryString()), null);

      return objectInstances.stream()
          .map(ObjectInstance::getObjectName)
          .map(BeanObject::new)
          .collect(Collectors.toSet());

    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (MalformedObjectNameException e) {
      // From the point we managed the BeanObject for library-user.
      // It is our responsibility to keep the object name string error-free, so the error shouldn't
      // be here.
      // If it actually happened, that means something wrong with the object name generation code.
      throw new IllegalStateException("Illegal object name detected, this shouldn't happens", e);
    }
  }

  private AttributeList getAttributeList(ObjectName objectName, String[] attributeName) {
    try {
      return mBeanServerConnection.getAttributes(objectName, attributeName);
    } catch (InstanceNotFoundException | ReflectionException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  void ensureClientConnected() {
    if (!isClientConnected) throw new IllegalStateException("client is not connected yet");
  }

  public void connect() {
    if (isClientConnected) return;

    try {
      this.jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
      this.mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      this.isClientConnected = true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    if (!isClientConnected) return;

    this.mBeanServerConnection = null;
    if (this.jmxConnector != null) {
      this.jmxConnector.close();
      this.jmxConnector = null;
    }

    isClientConnected = false;
  }
}
