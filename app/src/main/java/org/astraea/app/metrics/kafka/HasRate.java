package org.astraea.app.metrics.kafka;

import java.util.concurrent.TimeUnit;
import org.astraea.app.metrics.HasBeanObject;

public interface HasRate extends HasBeanObject {
  default double meanRate() {
    return (double) beanObject().getAttributes().getOrDefault("MeanRate", 0);
  }

  default double oneMinuteRate() {
    return (double) beanObject().getAttributes().getOrDefault("OneMinuteRate", 0);
  }

  default double fiveMinuteRate() {
    return (double) beanObject().getAttributes().getOrDefault("FiveMinuteRate", 0);
  }

  default double fifteenMinuteRate() {
    return (double) beanObject().getAttributes().getOrDefault("FifteenMinuteRate", 0);
  }

  default TimeUnit rateUnit() {
    return (TimeUnit) beanObject().getAttributes().get("RateUnit");
  }
}
