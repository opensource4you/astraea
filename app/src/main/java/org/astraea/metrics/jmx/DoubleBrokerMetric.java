package org.astraea.metrics.jmx;

public class DoubleBrokerMetric extends JmxBrokerMetric {
  public DoubleBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
