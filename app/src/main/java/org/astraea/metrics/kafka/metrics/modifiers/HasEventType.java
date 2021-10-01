package org.astraea.metrics.kafka.metrics.modifiers;

import java.util.Objects;

public interface HasEventType extends HasBeanObject {
  default String eventType() {
    return (String) Objects.requireNonNull(beanObject().getAttributes().get("EventType"));
  }
}
