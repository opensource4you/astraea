package org.astraea.metrics.jmx.types;

public class LongBrokerMetric extends JmxBrokerMetric {
  public LongBrokerMetric(String jmxObjectName, String attributeName)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
  }

  public LongBrokerMetric(
      String jmxObjectName, String attributeName, boolean objectNameResolutionRequired)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName, objectNameResolutionRequired);
  }
}
