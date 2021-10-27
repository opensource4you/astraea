package org.astraea.metrics.kafka.metrics;

import java.util.Map;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.metrics.modifiers.HasOperatingSystemInfo;

public class OperatingSystemInfo implements HasOperatingSystemInfo {

  private final BeanObject beanObject;

  public OperatingSystemInfo(BeanObject beanObject) {
    this.beanObject = beanObject;
  }

  @Override
  public BeanObject beanObject() {
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
    return "Operating System Info {" + sb + "}";
  }
}
