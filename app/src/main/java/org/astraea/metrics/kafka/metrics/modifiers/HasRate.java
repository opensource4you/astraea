package org.astraea.metrics.kafka.metrics.modifiers;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public interface HasRate extends HasBeanObject {
  default double meanRate() {
    return (double) Objects.requireNonNull(beanObject().getAttributes().get("MeanRate"));
  }

  default double oneMinuteRate() {
    return (double) Objects.requireNonNull(beanObject().getAttributes().get("OneMinuteRate"));
  }

  default double fiveMinuteRate() {
    return (double) Objects.requireNonNull(beanObject().getAttributes().get("FiveMinuteRate"));
  }

  default double fifteenMinuteRate() {
    return (double) Objects.requireNonNull(beanObject().getAttributes().get("FifteenMinuteRate"));
  }

  default TimeUnit rateUnit() {
    return (TimeUnit) Objects.requireNonNull(beanObject().getAttributes().get("RateUnit"));
  }
}
