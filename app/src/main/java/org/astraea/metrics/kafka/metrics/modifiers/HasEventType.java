package org.astraea.metrics.kafka.metrics.modifiers;

public interface HasEventType extends HasBeanObject {
  default String eventType() {
    return (String) beanObject().getAttributes().get("EventType");
  }
}
