package org.astraea.metrics.jmx.types;

public class IntegerBrokerMetric extends JmxBrokerMetric {

  public IntegerBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
