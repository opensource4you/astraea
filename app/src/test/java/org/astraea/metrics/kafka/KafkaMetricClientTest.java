package org.astraea.metrics.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;
import org.astraea.metrics.jmx.Utility;
import org.astraea.metrics.kafka.metrics.Metric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @Test
  void requestMetric() throws MalformedObjectNameException {
    // arrange
    Object mbean0 = Utility.createReadOnlyDynamicMBean(Map.of("Value", 100));
    Object mbean1 = Utility.createReadOnlyDynamicMBean(Map.of("Value", 2));
    Object mbean2 = Utility.createReadOnlyDynamicMBean(Map.of("Value", 3));
    ObjectName objectName0 = ObjectName.getInstance("org.example:type=category,index=0");
    ObjectName objectName1 = ObjectName.getInstance("org.example:type=category,index=1");
    ObjectName objectName2 = ObjectName.getInstance("org.example:type=category,index=2");
    register(objectName0, mbean0);
    register(objectName1, mbean1);
    register(objectName2, mbean2);

    // act
    Integer sum =
        sut.requestMetric(
            new Metric<>() {
              @Override
              public List<BeanQuery> queries() {
                return List.of(
                    BeanQuery.builder("org.example")
                        .property("type", "category")
                        .property("index", "0")
                        .build(),
                    BeanQuery.builder("org.example")
                        .property("type", "category")
                        .property("index", "1")
                        .build(),
                    BeanQuery.builder("org.example")
                        .property("type", "category")
                        .property("index", "2")
                        .build());
              }

              @Override
              public Integer from(List<BeanObject> beanObject) {
                return (Integer) beanObject.get(0).getAttributes().get("Value")
                    * (Integer) beanObject.get(1).getAttributes().get("Value")
                    / (Integer) beanObject.get(2).getAttributes().get("Value");
              }
            });

    // assert
    assertEquals(100 * 2 / 3, sum);
  }
}
