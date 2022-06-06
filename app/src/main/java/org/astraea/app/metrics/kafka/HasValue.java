package org.astraea.app.metrics.kafka;

import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.BeanObject;

public interface HasValue extends HasBeanObject {
  default long value() {
    var value = beanObject().getAttributes().getOrDefault("Value", 0);
    return ((Number) value).longValue();
  }

  static HasValue of(BeanObject beanObject) {
    return () -> beanObject;
  }
}
