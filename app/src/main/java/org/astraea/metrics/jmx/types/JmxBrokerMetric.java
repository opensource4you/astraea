package org.astraea.metrics.jmx.types;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class JmxBrokerMetric {

  private ObjectName jmxObjectName;
  private String attributeName;
  boolean objectNameResolutionRequired;

  public JmxBrokerMetric(String jmxObjectName, String attributeName) {
    this(jmxObjectName, attributeName, false);
  }

  public JmxBrokerMetric(
      String jmxObjectName, String attributeName, boolean objectNameResolutionRequired)
      throws IllegalArgumentException {
    try {
      this.jmxObjectName = ObjectName.getInstance(jmxObjectName);
      this.attributeName = attributeName;
      this.objectNameResolutionRequired = objectNameResolutionRequired;
    } catch (MalformedObjectNameException e) {
      // This kind of exception should be detected before used.
      String message = "Illegal JmxBrokerMetrics detected (%s, %s).";
      String formattedMessage = String.format(message, jmxObjectName, attributeName);
      throw new IllegalArgumentException(formattedMessage);
    }
  }

  public ObjectName getJmxObjectName() {
    return jmxObjectName;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public void resolveObjectName(ObjectName realName) {
    this.jmxObjectName = realName;
    this.objectNameResolutionRequired = false;
  }

  public boolean isObjectNameResolutionRequired() {
    return objectNameResolutionRequired;
  }
}
