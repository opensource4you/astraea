package org.astraea.metrics.kafka.metrics.modifiers;

import java.util.Objects;

public interface HasCount extends HasBeanObject {
  default long count() {
    return (long) Objects.requireNonNull(beanObject().getAttributes().getOrDefault("Count", 0));
  }
}
