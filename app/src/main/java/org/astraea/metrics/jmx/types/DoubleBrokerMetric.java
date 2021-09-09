package org.astraea.metrics.jmx.types;

public class DoubleBrokerMetric extends JmxBrokerMetric {
  public DoubleBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
