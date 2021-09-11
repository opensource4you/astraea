package org.astraea.metrics.jmx;

import java.io.IOException;
import java.net.MalformedURLException;
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
      String objectName = beanObject.jmxQueryString();
      String[] attributeNameArray = beanObject.getAttributeView().keySet().toArray(new String[0]);

      // fetch attribute list, create a new object based on these new attribute to keep everything
      // immutable.
      AttributeList attributeList =
          getAttributeList(ObjectName.getInstance(objectName), attributeNameArray);
      return new BeanObject(beanObject, attributeList);

    } catch (MalformedObjectNameException e) {
      // From the point we managed the BeanObject for library-user.
      // It is our responsibility to keep the jmxUrl error-free, so the error shouldn't be here.
      // If it actually happened, that means something wrong with the JMX Url generation code.
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
