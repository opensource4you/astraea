package org.astraea.metrics.kafka.metrics;

import java.util.Map;
import java.util.Objects;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.metrics.kafka.metrics.modifiers.*;

public class BrokerTopicMetricsResult implements HasCount, HasEventType, HasRate {

  private final BeanObject beanObject;
  private final KafkaMetrics.BrokerTopicMetrics metric;

  public BrokerTopicMetricsResult(KafkaMetrics.BrokerTopicMetrics metric, BeanObject beanObject) {
    this.beanObject = Objects.requireNonNull(beanObject);
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

  @Override
  public BeanObject beanObject() {
    return beanObject;
  }
}
