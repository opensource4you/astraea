package org.astraea.metrics.jmx.types;

public class DoubleBrokerMetric extends JmxBrokerMetric {
  public DoubleBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }

  public DoubleBrokerMetric(
      String jmxObjectName, String attributeName, boolean objectNameResolutionRequired)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName, objectNameResolutionRequired);
  }
}
