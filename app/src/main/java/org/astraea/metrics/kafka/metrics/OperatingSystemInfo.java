package org.astraea.metrics.kafka.metrics;

import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.metrics.modifiers.HasOperatingSystemInfo;

public class OperatingSystemInfo implements HasOperatingSystemInfo {

  private final BeanObject beanObject;

  public OperatingSystemInfo(BeanObject beanObject) {
    this.beanObject = beanObject;
  }

  @Override
  public BeanObject beanObject() {
    return beanObject;
  }
}
