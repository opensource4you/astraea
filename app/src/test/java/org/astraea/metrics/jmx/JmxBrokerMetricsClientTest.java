package org.astraea.metrics.jmx;

import java.net.MalformedURLException;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JmxBrokerMetricsClientTest {

  @Test
  void testFetchMetricWithoutConnectWillThrowError() throws MalformedURLException {
    final String JMX_URI_FORMAT = "service:jmx:rmi:///jndi/rmi://" + "%s" + ":" + "%d" + "/jmxrmi";
    final String url = String.format(JMX_URI_FORMAT, "example.com", 5566);
    JMXServiceURL jmxServiceURL = new JMXServiceURL(url);

    JmxBrokerMetricsClient client = new JmxBrokerMetricsClient(jmxServiceURL);

    Assertions.assertThrows(
        IllegalStateException.class, () -> client.fetchJmxMetric(new JmxBrokerMetric("", "")));
    Assertions.assertThrows(
        IllegalStateException.class, () -> client.fetchMetric(new DoubleBrokerMetric("", "")));
    Assertions.assertThrows(
        IllegalStateException.class, () -> client.fetchMetric(new IntegerBrokerMetric("", "")));
    Assertions.assertThrows(
        IllegalStateException.class, () -> client.fetchMetric(new LongBrokerMetric("", "")));
    Assertions.assertThrows(
        IllegalStateException.class, () -> client.fetchMetric(new StringBrokerMetric("", "")));
    Assertions.assertThrows(
        IllegalStateException.class, () -> client.fetchMetric(new CompositeDataMetric("", "")));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> client.fetchMetric(new CustomCompositeDataMetric<>("", "", (x) -> null)));
  }
}
