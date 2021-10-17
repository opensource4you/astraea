package org.astraea.metrics.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.MBeanClient;
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
    BeanQuery query = metric.query();
    BeanObject resolved = mBeanClient.tryQueryBean(query).orElseThrow();
    return metric.from(resolved);
  }

  @Override
  public void close() throws Exception {
    this.mBeanClient.close();
  }
}
