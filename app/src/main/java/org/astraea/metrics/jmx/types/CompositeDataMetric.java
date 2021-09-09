package org.astraea.metrics.jmx.types;

public class CompositeDataMetric extends JmxBrokerMetric {

  public CompositeDataMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
