package org.astraea.metrics.jmx;

public class CompositeDataMetric extends JmxBrokerMetric {

  public CompositeDataMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
