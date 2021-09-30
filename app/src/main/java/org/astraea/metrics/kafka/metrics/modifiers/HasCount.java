package org.astraea.metrics.kafka.metrics.modifiers;

public interface HasCount extends HasBeanObject {
  default long count() {
    return (long) beanObject().getAttributes().get("Count");
  }
}
