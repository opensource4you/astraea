package org.astraea.app.metrics.kafka;

import org.astraea.app.metrics.HasBeanObject;

public interface HasCount extends HasBeanObject {
  default long count() {
    return (long) beanObject().getAttributes().getOrDefault("Count", 0);
  }
}
