package org.astraea.app.metrics.kafka;

import java.util.Map;
import java.util.Objects;
import org.astraea.app.metrics.jmx.BeanObject;

public class BrokerTopicMetricsResult implements HasCount, HasEventType, HasRate {

  private final BeanObject beanObject;

  public BrokerTopicMetricsResult(BeanObject beanObject) {
    this.beanObject = Objects.requireNonNull(beanObject);
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
    return beanObject().getProperties().get("name") + "{" + sb + "}";
  }

  @Override
  public BeanObject beanObject() {
    return beanObject;
  }
}
