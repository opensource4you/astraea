package org.astraea.metrics.kafka.metrics;

import java.util.Map;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.metrics.modifiers.HasCount;
import org.astraea.metrics.kafka.metrics.modifiers.HasPercentiles;
import org.astraea.metrics.kafka.metrics.modifiers.HasStatistics;

public class TotalTimeMs implements HasPercentiles, HasCount, HasStatistics {

  private final BeanObject beanObject;

  public TotalTimeMs(BeanObject beanObject) {
    this.beanObject = beanObject;
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
    return beanObject.getProperties().get("request") + " TotalTimeMs {" + sb + "}";
  }

  @Override
  public BeanObject beanObject() {
    return beanObject;
  }
}
