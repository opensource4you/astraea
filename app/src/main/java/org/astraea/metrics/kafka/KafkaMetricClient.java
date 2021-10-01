package org.astraea.metrics.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;

public class KafkaMetricClient implements AutoCloseable {

  private final MBeanClient mBeanClient;

  public KafkaMetricClient(JMXServiceURL serviceURL) {
    try {
      this.mBeanClient = new MBeanClient(serviceURL);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Request BrokerTopicMetrics.
   *
   * <p>For the available metrics, checkout {@link BrokerTopicMetrics}.
   *
   * @param metric the metric to request
   * @return a {@link BrokerTopicMetricsResult} indicate the result
   */
  public BrokerTopicMetricsResult requestMetric(BrokerTopicMetrics metric) {
    BeanObject beanObject =
        mBeanClient
            .tryQueryBean(
                BeanQuery.builder("kafka.server")
                    .property("type", "BrokerTopicMetrics")
                    .property("name", metric.metricName())
                    .build())
            .orElseThrow();

    return new BrokerTopicMetricsResult(metric, beanObject);
  }

  @Override
  public void close() throws Exception {
    this.mBeanClient.close();
  }
}
