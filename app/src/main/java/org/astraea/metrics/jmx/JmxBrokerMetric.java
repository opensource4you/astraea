package org.astraea.metrics.jmx;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class JmxBrokerMetric {

  public final ObjectName jmxObjectName;
  public final String attributeName;

  JmxBrokerMetric(String jmxObjectName, String attributeName) throws IllegalArgumentException {
    try {
      this.jmxObjectName = ObjectName.getInstance(jmxObjectName);
      this.attributeName = attributeName;
    } catch (MalformedObjectNameException e) {
      // This kind of exception should be detected before used.
      String message = "Illegal JmxBrokerMetrics detected (%s, %s).";
      String formattedMessage = String.format(message, jmxObjectName, attributeName);
      throw new IllegalArgumentException(formattedMessage);
    }
  }
}
