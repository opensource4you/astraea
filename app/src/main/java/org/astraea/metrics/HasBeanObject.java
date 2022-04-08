package org.astraea.metrics;

import org.astraea.metrics.jmx.BeanObject;

public interface HasBeanObject {
  BeanObject beanObject();

  default long createdTimestamp() {
    return beanObject().createdTimestamp();
  }
}
