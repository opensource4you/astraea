package org.astraea.app.metrics.kafka;

import org.astraea.app.metrics.HasBeanObject;

public interface HasPercentiles extends HasBeanObject {

  default double percentile50() {
    return (double) beanObject().getAttributes().get("50thPercentile");
  }

  default double percentile75() {
    return (double) beanObject().getAttributes().get("75thPercentile");
  }

  default double percentile95() {
    return (double) beanObject().getAttributes().get("95thPercentile");
  }

  default double percentile98() {
    return (double) beanObject().getAttributes().get("98thPercentile");
  }

  default double percentile99() {
    return (double) beanObject().getAttributes().get("99thPercentile");
  }

  default double percentile999() {
    return (double) beanObject().getAttributes().get("999thPercentile");
  }
}
