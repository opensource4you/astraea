package org.astraea.metrics.jmx;

import java.io.IOException;
import javax.management.*;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

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
    final ObjectName metricName = metric.jmxObjectName;
    final String metricAttribute = metric.attributeName;

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
