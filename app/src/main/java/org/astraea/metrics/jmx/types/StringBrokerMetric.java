package org.astraea.metrics.jmx.types;

public class StringBrokerMetric extends JmxBrokerMetric {

  public StringBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }

  public StringBrokerMetric(
      String jmxObjectName, String attributeName, boolean objectNameResolutionRequired)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName, objectNameResolutionRequired);
  }
}
