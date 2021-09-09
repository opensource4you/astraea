package org.astraea.metrics.jmx.template;

import org.astraea.metrics.jmx.DoubleBrokerMetric;
import org.astraea.metrics.jmx.LongBrokerMetric;

public interface StatisticsMetricsTemplate extends MetricTemplate {

  default DoubleBrokerMetric percentile50() {
    return new DoubleBrokerMetric(createJmxName(), "50thPercentile");
  }

  default DoubleBrokerMetric percentile75() {
    return new DoubleBrokerMetric(createJmxName(), "75thPercentile");
  }

  default DoubleBrokerMetric percentile95() {
    return new DoubleBrokerMetric(createJmxName(), "95thPercentile");
  }

  default DoubleBrokerMetric percentile98() {
    return new DoubleBrokerMetric(createJmxName(), "98thPercentile");
  }

  default DoubleBrokerMetric percentile99() {
    return new DoubleBrokerMetric(createJmxName(), "99thPercentile");
  }

  default DoubleBrokerMetric percentile999() {
    return new DoubleBrokerMetric(createJmxName(), "999thPercentile");
  }

  default LongBrokerMetric count() {
    return new LongBrokerMetric(createJmxName(), "Count");
  }

  default DoubleBrokerMetric max() {
    return new DoubleBrokerMetric(createJmxName(), "Max");
  }

  default DoubleBrokerMetric min() {
    return new DoubleBrokerMetric(createJmxName(), "Min");
  }

  default DoubleBrokerMetric mean() {
    return new DoubleBrokerMetric(createJmxName(), "Mean");
  }

  default DoubleBrokerMetric stddev() {
    return new DoubleBrokerMetric(createJmxName(), "StdDev");
  }
}
