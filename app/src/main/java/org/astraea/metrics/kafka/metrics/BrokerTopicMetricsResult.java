package org.astraea.metrics.kafka.metrics;

import java.util.Map;
import java.util.Objects;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.metrics.modifiers.HasCount;
import org.astraea.metrics.kafka.metrics.modifiers.HasEventType;
import org.astraea.metrics.kafka.metrics.modifiers.HasRate;
import org.astraea.metrics.kafka.metrics.modifiers.MetricsResult;

public class BrokerTopicMetricsResult extends MetricsResult
    implements HasCount, HasEventType, HasRate {

  private final BrokerTopicMetrics metric;

  public BrokerTopicMetricsResult(BrokerTopicMetrics metric, BeanObject beanObject) {
    super(Objects.requireNonNull(beanObject));
    this.metric = Objects.requireNonNull(metric);
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
