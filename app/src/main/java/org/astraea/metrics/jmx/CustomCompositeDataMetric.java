package org.astraea.metrics.jmx;

import java.util.function.Function;
import javax.management.openmbean.CompositeDataSupport;

public class CustomCompositeDataMetric<T> extends JmxBrokerMetric {

  private final Function<CompositeDataSupport, T> transformer;

  public CustomCompositeDataMetric(
      String jmxObjectName, String attributeName, Function<CompositeDataSupport, T> transformer)
      throws IllegalArgumentException {
    super(jmxObjectName, attributeName);
    this.transformer = transformer;
  }

  public T transform(CompositeDataSupport jmxCompositeData) {
    return transformer.apply(jmxCompositeData);
  }
}
