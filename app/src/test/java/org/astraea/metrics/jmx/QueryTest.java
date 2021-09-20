package org.astraea.metrics.jmx;

import static org.astraea.metrics.jmx.KafkaMetrics.RequestMetrics.*;
import static org.astraea.metrics.jmx.KafkaMetrics.Requests.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryTest {

  private MBeanServer mBeanServer;
  private JMXConnectorServer jmxServer;

  @BeforeEach
  void setUp() throws IOException {
    JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");

    mBeanServer = ManagementFactory.getPlatformMBeanServer();
    jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServer);
    jmxServer.start();
  }

  @AfterEach
  void tearDown() throws IOException {
    jmxServer.stop();
    mBeanServer = null;
  }

  @Test
  void demoHowtoImplementCustomDSL() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {
      mBeanServer.registerMBean(
          new Demo(0, 100, 50),
          ObjectName.getInstance(
              "kafka.network:type=RequestMetrics,request=Fetch,name=RequestBytes"));
      Map<String, String> expectedProperties =
          Map.of(
              "type", "RequestMetrics",
              "request", "Fetch",
              "name", "RequestBytes");
      Map<String, Object> expectedAttributes =
          Map.of(
              "Min", 0L,
              "Max", 100L,
              "Mean", 50.0);

      // act
      BeanObject beanObject = client.queryBean(KafkaMetrics.ofRequest(Fetch, RequestBytes));

      // assert
      assertEquals(expectedProperties, beanObject.getProperties());
      assertEquals(expectedAttributes, beanObject.getAttributes());
    }
  }

  public interface DemoMBean {
    void setMax(long max);

    void setMin(long min);

    void setMean(double mean);

    long getMax();

    long getMin();

    double getMean();
  }

  static class Demo implements DemoMBean {
    private long min, max;
    private double mean;

    @Override
    public void setMin(long min) {
      this.min = min;
    }

    @Override
    public void setMax(long max) {
      this.max = max;
    }

    @Override
    public void setMean(double mean) {
      this.mean = mean;
    }

    @Override
    public long getMin() {
      return min;
    }

    @Override
    public long getMax() {
      return max;
    }

    @Override
    public double getMean() {
      return mean;
    }

    Demo(long min, long max, double mean) {
      this.min = min;
      this.max = max;
      this.mean = mean;
    }
  }
}

// Declare multiple class inside one java file is discouraged. We only doing this for demo purpose.
class KafkaMetrics implements Query {

  public enum Requests {
    Fetch,
    // FetchConsumer,
    // FetchFollower,
    // Produce,
    // ...
  }

  public enum RequestMetrics {
    RequestBytes,
    // LocalTimeMs,
    // RemoteTimesMs,
    // RequestQueueTimeMs,
    // ResponseQueueTimeMs,
    // ResponseSendTimeMs,
    // ThrottleTimeMs,
    // TotalTimeMs,
  }

  private final String domainName;
  private final Map<String, String> properties;
  private final ObjectName objectName;

  private KafkaMetrics(BeanQuery beanQuery) {
    this.domainName = beanQuery.domainName();
    this.properties = Map.copyOf(beanQuery.properties());
    this.objectName = beanQuery.getQuery();
  }

  @Override
  public ObjectName getQuery() {
    return objectName;
  }

  @Override
  public String domainName() {
    return domainName;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  static BeanQuery ofRequest(Requests requestName, RequestMetrics metrics) {
    return BeanQuery.of("kafka.network")
        .whereProperty("type", "RequestMetrics")
        .whereProperty("request", requestName.name())
        .whereProperty("name", metrics.name())
        .build();
  }
}
