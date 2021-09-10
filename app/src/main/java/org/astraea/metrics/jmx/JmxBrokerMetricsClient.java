package org.astraea.metrics.jmx;

import java.io.IOException;
import java.util.Set;
import javax.management.*;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.types.*;

public class JmxBrokerMetricsClient implements AutoCloseable {

  private final JMXServiceURL jmxUrl;
  private JMXConnector jmxConnector;
  private MBeanServerConnection mBeanServerConnection;
  private boolean notConnectedYet = true;

  public JmxBrokerMetricsClient(JMXServiceURL jmxUrl) {
    this.jmxUrl = jmxUrl;
  }

  public void connect() throws IOException {
    this.jmxConnector = JMXConnectorFactory.connect(this.jmxUrl);
    this.mBeanServerConnection = this.jmxConnector.getMBeanServerConnection();
    this.notConnectedYet = false;
  }

  public Object fetchJmxMetric(JmxBrokerMetric metric) {
    throwExceptionIfNotConnectedYet();

    if (metric.isObjectNameResolutionRequired()) {
      ObjectName realName = attemptToResolveObjectName(metric);
      metric.resolveObjectName(realName);
    }

    final ObjectName metricName = metric.getJmxObjectName();
    final String metricAttribute = metric.getAttributeName();

    try {
      return mBeanServerConnection.getAttribute(metricName, metricAttribute);
    } catch (MBeanException
        | AttributeNotFoundException
        | InstanceNotFoundException
        | ReflectionException
        | IOException e) {
      throw new MBeanResolutionException(e);
    }
  }

  private ObjectName attemptToResolveObjectName(JmxBrokerMetric metric) {
    Set<ObjectInstance> objectInstances = null;
    try {
      objectInstances = this.mBeanServerConnection.queryMBeans(metric.getJmxObjectName(), null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (objectInstances.isEmpty())
      throw new IllegalArgumentException(
          String.format(
              "Target jmx name '%s' match nothing on Mbean server", metric.getJmxObjectName()));
    return objectInstances.stream().findFirst().get().getObjectName();
  }

  public long fetchMetric(LongBrokerMetric metric) {
    return (long) fetchJmxMetric(metric);
  }

  public int fetchMetric(IntegerBrokerMetric metric) {
    return (int) fetchJmxMetric(metric);
  }

  public double fetchMetric(DoubleBrokerMetric metric) {
    return (double) fetchJmxMetric(metric);
  }

  public String fetchMetric(StringBrokerMetric metric) {
    return (String) fetchJmxMetric(metric);
  }

  public CompositeDataSupport fetchMetric(CompositeDataMetric metric) {
    return (CompositeDataSupport) fetchJmxMetric(metric);
  }

  public <T> T fetchMetric(CustomCompositeDataMetric<T> metric) {
    return metric.transform((CompositeDataSupport) fetchJmxMetric(metric));
  }

  void throwExceptionIfNotConnectedYet() {
    if (notConnectedYet) throw new IllegalStateException("You need to connect to JMX agent first");
  }

  @Override
  public void close() throws Exception {
    this.jmxConnector.close();
  }
}
