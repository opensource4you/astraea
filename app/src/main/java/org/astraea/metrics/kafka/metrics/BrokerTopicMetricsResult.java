package org.astraea.metrics.kafka.metrics;

import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.metrics.modifiers.HasCount;
import org.astraea.metrics.kafka.metrics.modifiers.HasEventType;
import org.astraea.metrics.kafka.metrics.modifiers.HasRate;
import org.astraea.metrics.kafka.metrics.modifiers.MetricsResult;

import java.util.Map;

public class BrokerTopicMetricsResult extends MetricsResult
    implements HasCount, HasEventType, HasRate {

  private final BrokerTopicMetrics metric;

  public BrokerTopicMetricsResult(BrokerTopicMetrics metric, BeanObject beanObject) {
    super(beanObject);
    this.metric = metric;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> e : beanObject().getAttributes().entrySet()) {
      sb.append(System.lineSeparator())
              .append("  ")
              .append(e.getKey())
              .append("=")
              .append(e.getValue());
    }
    return metric.name() + "{" + sb + "}";
  }
}
