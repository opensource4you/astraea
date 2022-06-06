package org.astraea.app.metrics.kafka;

import org.astraea.app.metrics.HasBeanObject;

public interface HasEventType extends HasBeanObject {
  default String eventType() {
    return (String) beanObject().getAttributes().get("EventType");
  }
}
