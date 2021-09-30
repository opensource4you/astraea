package org.astraea.metrics.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.Utility;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetrics;
import org.astraea.metrics.kafka.metrics.BrokerTopicMetricsResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class KafkaMetricClientTest {

  private MBeanServer mBeanServer;
  private JMXConnectorServer jmxServer;
  private Map<ObjectName, Object> registeredBeans = new HashMap<>();
  private KafkaMetricClient sut;

  private void register(ObjectName name, Object mBean) {
    registeredBeans.put(name, mBean);
    try {
      mBeanServer.registerMBean(mBean, name);
    } catch (InstanceAlreadyExistsException
        | MBeanRegistrationException
        | NotCompliantMBeanException e) {
      throw new RuntimeException(e);
    }
  }

  private void clearRegisteredMBeans() {
    registeredBeans.forEach(
        (name, mbeans) -> {
          try {
            mBeanServer.unregisterMBean(name);
          } catch (InstanceNotFoundException | MBeanRegistrationException e) {
            throw new RuntimeException(e);
          }
        });
    registeredBeans.clear();
  }

  @BeforeEach
  void setUp() throws IOException {
    JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");

    mBeanServer = ManagementFactory.getPlatformMBeanServer();

    jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServer);
    jmxServer.start();

    sut = new KafkaMetricClient(jmxServer.getAddress());
  }

  @AfterEach
  void tearDown() throws Exception {
    jmxServer.stop();
    clearRegisteredMBeans();
    mBeanServer = null;
    sut.close();
  }

  @ParameterizedTest
  @EnumSource(value = BrokerTopicMetrics.class)
  void testRequestBrokerTopicMetrics(BrokerTopicMetrics metric)
      throws MalformedObjectNameException {
    // arrange
    Object mbean =
        Utility.createReadOnlyDynamicMBean(
            Map.of(
                "Count", 1L,
                "EventType", "bytes",
                "FifteenMinuteRate", 2.0,
                "FiveMinuteRate", 4.0,
                "MeanRate", 8.0,
                "OneMinuteRate", 16.0,
                "RateUnit", "SECONDS"));
    register(
        ObjectName.getInstance("kafka.server:type=BrokerTopicMetrics,name=" + metric.name()),
        mbean);

    // act
    BrokerTopicMetricsResult result = sut.requestMetric(metric);

    // assert
    assertEquals(1L, result.count());
    assertEquals("bytes", result.eventType());
    assertEquals(2.0, result.fifteenMinuteRate());
    assertEquals(4.0, result.fiveMinuteRate());
    assertEquals(8.0, result.meanRate());
    assertEquals(16.0, result.oneMinuteRate());
    assertEquals("SECONDS", result.rateUnit());
  }
}
