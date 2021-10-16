package org.astraea.metrics.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;
import java.util.List;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.metrics.Metric;

public class KafkaMetricClient implements AutoCloseable {

  private final MBeanClient mBeanClient;

  public KafkaMetricClient(JMXServiceURL serviceURL) {
    try {
      this.mBeanClient = new MBeanClient(serviceURL);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public <RET_TYPE> RET_TYPE requestMetric(Metric<RET_TYPE> metric) {

    List<BeanQuery> queries = metric.queries();
    List<BeanObject> resolved = new LinkedList<>();

    for (BeanQuery query : queries) {
      resolved.add(mBeanClient.tryQueryBean(query).orElseThrow());
    }

    return metric.from(resolved);
  }

  /**
   * Request BrokerTopicMetrics.
   *
   * <p>For the available metrics, checkout {@link BrokerTopicMetrics}.
   *
   * @deprecated As this method couple the fetch/transform operation in the {@link
   *     KafkaMetricClient} design, replaced by {@link #requestMetric(Metric)}
   * @param metric the metric to request
   * @return a {@link BrokerTopicMetricsResult} indicate the result
   */
  @Deprecated
  public BrokerTopicMetricsResult requestMetric(BrokerTopicMetrics metric) {
    // Java determine overloaded method call in compile-time, must do the transformation here to
    // ensure we are calling the right method implementation.
    return this.requestMetric((Metric<BrokerTopicMetricsResult>) metric);
  }

  @Override
  public void close() throws Exception {
    this.mBeanClient.close();
  }
}
