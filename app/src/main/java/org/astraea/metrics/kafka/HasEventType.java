package org.astraea.metrics.kafka;

import org.astraea.metrics.HasBeanObject;

public interface HasEventType extends HasBeanObject {
  default String eventType() {
    return (String) beanObject().getAttributes().get("EventType");
  }
}
