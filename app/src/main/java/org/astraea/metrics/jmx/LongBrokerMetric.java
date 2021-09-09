package org.astraea.metrics.jmx;

public class LongBrokerMetric extends JmxBrokerMetric {
  public LongBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
