package org.astraea.metrics.jmx;

public class StringBrokerMetric extends JmxBrokerMetric {

  StringBrokerMetric(String jmxObjectName, String attributeName) throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }
}
