package org.astraea.metrics.kafka;

import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;

public interface HasValue extends HasBeanObject {
  default long value() {
    var value = beanObject().getAttributes().getOrDefault("Value", 0);
    return ((Number) value).longValue();
  }

  static HasValue of(BeanObject beanObject) {
    return () -> beanObject;
  }
}
