package org.astraea.metrics.kafka;

import org.astraea.metrics.HasBeanObject;

public interface HasCount extends HasBeanObject {
  default long count() {
    return (long) beanObject().getAttributes().getOrDefault("Count", 0);
  }
}
