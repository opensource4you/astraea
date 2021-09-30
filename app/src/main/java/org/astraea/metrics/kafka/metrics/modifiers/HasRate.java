package org.astraea.metrics.kafka.metrics.modifiers;

public interface HasRate extends HasBeanObject {
  default double meanRate() {
    return (double) beanObject().getAttributes().get("MeanRate");
  }

  default double oneMinuteRate() {
    return (double) beanObject().getAttributes().get("OneMinuteRate");
  }

  default double fiveMinuteRate() {
    return (double) beanObject().getAttributes().get("FiveMinuteRate");
  }

  default double fifteenMinuteRate() {
    return (double) beanObject().getAttributes().get("FifteenMinuteRate");
  }

  default String rateUnit() {
    return (String) beanObject().getAttributes().get("RateUnit");
  }
}
