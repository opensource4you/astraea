package org.astraea.metrics.jmx;

public class CompositeDataMetric extends JmxBrokerMetric {

  CompositeDataMetric(String jmxObjectName, String attributeName) throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
