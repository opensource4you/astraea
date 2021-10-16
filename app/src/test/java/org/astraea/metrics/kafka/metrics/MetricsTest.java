package org.astraea.metrics.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.Utility;
import org.astraea.metrics.kafka.KafkaMetricClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class MetricsTest {

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
                "RateUnit", TimeUnit.SECONDS));
    register(
        ObjectName.getInstance("kafka.server:type=BrokerTopicMetrics,name=" + metric.name()),
        mbean);

    // act
    BrokerTopicMetricsResult result = sut.requestMetric(Metrics.brokerTopicMetric(metric));

    // assert
    assertEquals(1L, result.count());
    assertEquals("bytes", result.eventType());
    assertEquals(2.0, result.fifteenMinuteRate());
    assertEquals(4.0, result.fiveMinuteRate());
    assertEquals(8.0, result.meanRate());
    assertEquals(16.0, result.oneMinuteRate());
    assertEquals(TimeUnit.SECONDS, result.rateUnit());
  }

  @ParameterizedTest()
  @EnumSource(value = PurgatoryRequest.class)
  void testPurgatorySize(PurgatoryRequest request) throws MalformedObjectNameException {
    // arrange
    Object mbean = Utility.createReadOnlyDynamicMBean(Map.of("Value", 0xfee1dead));
    register(
        ObjectName.getInstance(
            "kafka.server:type=DelayedOperationPurgatory,delayedOperation="
                + request.name()
                + ",name=PurgatorySize"),
        mbean);

    // act
    Integer value = sut.requestMetric(Metrics.Purgatory.size(request));

    // assert
    assertEquals(0xfee1dead, value);
  }

  @ParameterizedTest()
  @EnumSource(value = Metrics.RequestMetrics.RequestTotalTimeMs.class)
  void testRequestTotalTimeMs(Metrics.RequestMetrics.RequestTotalTimeMs request)
      throws MalformedObjectNameException {
    // arrange
    Map<String, Object> map = new HashMap<>();
    map.put("50thPercentile", 1.0);
    map.put("75thPercentile", 2.0);
    map.put("95thPercentile", 3.0);
    map.put("98thPercentile", 4.0);
    map.put("99thPercentile", 5.0);
    map.put("999thPercentile", 6.0);
    map.put("Count", 7L);
    map.put("Max", 8.0);
    map.put("Mean", 9.0);
    map.put("Min", 10.0);
    map.put("StdDev", 11.0);
    Object mbean = Utility.createReadOnlyDynamicMBean(map);
    register(
        ObjectName.getInstance(
            "kafka.network:type=RequestMetrics,request=" + request.name() + ",name=TotalTimeMs"),
        mbean);

    // act
    TotalTimeMs totalTimeMs = sut.requestMetric(Metrics.RequestMetrics.totalTimeMs(request));

    // assert
    assertEquals(totalTimeMs.percentile50(), 1.0);
    assertEquals(totalTimeMs.percentile75(), 2.0);
    assertEquals(totalTimeMs.percentile95(), 3.0);
    assertEquals(totalTimeMs.percentile98(), 4.0);
    assertEquals(totalTimeMs.percentile99(), 5.0);
    assertEquals(totalTimeMs.percentile999(), 6.0);
    assertEquals(totalTimeMs.count(), 7L);
    assertEquals(totalTimeMs.max(), 8.0);
    assertEquals(totalTimeMs.mean(), 9.0);
    assertEquals(totalTimeMs.min(), 10.0);
    assertEquals(totalTimeMs.stdDev(), 11.0);
  }

  @Test
  void testGlobalPartitionCount() throws MalformedObjectNameException {
    // arrange
    Object mbean = Utility.createReadOnlyDynamicMBean(Map.of("Value", 0xcafebabe));
    register(
        ObjectName.getInstance("kafka.controller:type=KafkaController,name=GlobalPartitionCount"),
        mbean);

    // act
    Integer integer = sut.requestMetric(Metrics.TopicPartition.globalPartitionCount());

    // assert
    assertEquals(0xcafebabe, integer);
  }

  @Test
  void testUnderReplicatedPartitions() throws MalformedObjectNameException {
    // arrange
    Object mbean = Utility.createReadOnlyDynamicMBean(Map.of("Value", 0x55665566));
    register(
        ObjectName.getInstance("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"),
        mbean);

    // act
    Integer integer = sut.requestMetric(Metrics.TopicPartition.underReplicatedPartitions());

    // assert
    assertEquals(0x55665566, integer);
  }
}
