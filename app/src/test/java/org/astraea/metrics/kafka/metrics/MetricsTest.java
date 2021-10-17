package org.astraea.metrics.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.MBeanClient;
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
  private MBeanClient mBeanClient;

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
    mBeanClient = new MBeanClient(jmxServer.getAddress());
  }

  @AfterEach
  void tearDown() throws Exception {
    jmxServer.stop();
    clearRegisteredMBeans();
    mBeanServer = null;
    sut.close();
  }

  @ParameterizedTest
  @EnumSource(value = Metrics.BrokerTopicMetrics.class)
  void testRequestBrokerTopicMetrics(Metrics.BrokerTopicMetrics metric)
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
    BrokerTopicMetricsResult result = metric.fetch(mBeanClient);

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
  @EnumSource(value = Metrics.Purgatory.class)
  void testPurgatorySize(Metrics.Purgatory request) throws MalformedObjectNameException {
    // arrange
    Function<Integer, Object> fMbean =
        (a) -> Utility.createReadOnlyDynamicMBean(Map.of("Value", a));
    Function<String, String> fname =
        (a) ->
            String.format(
                "kafka.server:type=DelayedOperationPurgatory,delayedOperation=%s,name=PurgatorySize",
                a);
    register(ObjectName.getInstance(fname.apply(request.name())), fMbean.apply(0xcafebabe));

    // act
    int size = request.size(mBeanClient);

    // assert
    assertEquals(0xcafebabe, size);
  }

  @ParameterizedTest()
  @EnumSource(value = Metrics.RequestMetrics.class)
  void testRequestTotalTimeMs(Metrics.RequestMetrics request) throws MalformedObjectNameException {
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
    TotalTimeMs totalTimeMs = request.totalTimeMs(mBeanClient);

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
    int integer = Metrics.TopicPartition.globalPartitionCount(mBeanClient);

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
    int integer = Metrics.TopicPartition.underReplicatedPartitions(mBeanClient);

    // assert
    assertEquals(0x55665566, integer);
  }
}
