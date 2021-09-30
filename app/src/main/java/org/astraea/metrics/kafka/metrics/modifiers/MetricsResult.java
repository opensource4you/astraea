package org.astraea.metrics.kafka.metrics.modifiers;

import java.util.Map;
import org.astraea.metrics.jmx.BeanObject;

public class MetricsResult implements HasBeanObject {

  private final BeanObject beanObject;

  protected MetricsResult(BeanObject beanObject) {
    this.beanObject = beanObject;
  }

  public final BeanObject beanObject() {
    return beanObject;
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
    return this.getClass().getSimpleName() + "{" + sb + "}";
  }
}
