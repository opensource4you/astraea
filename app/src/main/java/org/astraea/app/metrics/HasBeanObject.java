package org.astraea.app.metrics;

import org.astraea.app.metrics.jmx.BeanObject;

public interface HasBeanObject {
  BeanObject beanObject();

  default long createdTimestamp() {
    return beanObject().createdTimestamp();
  }
}
