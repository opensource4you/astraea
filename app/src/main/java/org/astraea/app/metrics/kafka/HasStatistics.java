package org.astraea.app.metrics.kafka;

import org.astraea.app.metrics.HasBeanObject;

public interface HasStatistics extends HasBeanObject {

  default double max() {
    return (double) beanObject().getAttributes().get("Max");
  }

  default double min() {
    return (double) beanObject().getAttributes().get("Min");
  }

  default double mean() {
    return (double) beanObject().getAttributes().get("Mean");
  }

  default double stdDev() {
    return (double) beanObject().getAttributes().get("StdDev");
  }
}
