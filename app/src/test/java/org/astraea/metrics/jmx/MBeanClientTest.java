package org.astraea.metrics.jmx;

import static org.astraea.metrics.jmx.TestUtility.MBeansDataset.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MBeanClientTest {

  @Test
  void useMBeanClientWithoutConnectFirstWillThrowError() throws MalformedURLException {
    final MBeanClient mBeanClient =
        new MBeanClient("service:jmx:rmi:///jndi/rmi://example:5566/jmxrmi");
    final BeanObject beanObject = new BeanObject("");
    final BeanQuery beanQuery = new BeanQuery("");

    assertThrows(IllegalStateException.class, () -> mBeanClient.fetchObjectAttribute(beanObject));
    assertThrows(IllegalStateException.class, () -> mBeanClient.queryObject(beanQuery));
  }

  @Nested
  static class CanQuery_CanFetchAttribute {

    private static JMXConnectorServer jmxConnectorServer;
    private static MBeanClient client;

    @BeforeAll
    static void setup() throws IOException {
      jmxConnectorServer = TestUtility.setupJmxServerSpy(mbeansServerContent);
      jmxConnectorServer.start();
      client = new MBeanClient(jmxConnectorServer.getAddress());
      client.connect();
    }

    @AfterAll
    static void teardown() throws Exception {
      client.close();
      jmxConnectorServer.stop();
    }

    static Stream<Arguments> dataset1() {
      BeanQuery beanQuery1 =
          BeanQuery.forDomainName("org.example").whereProperty("type", "object1");
      BeanQuery beanQuery2 =
          BeanQuery.forDomainName("org.example").whereProperty("type", "object?");
      BeanQuery beanQuery3 =
          BeanQuery.forDomainName("org.example").whereProperty("type", "object*");
      BeanQuery beanQuery4 = BeanQuery.forDomainName("org.*").whereProperty("type", "object*");
      return Stream.of(
          Arguments.of(beanQuery1, Set.of(objectName1)),
          Arguments.of(beanQuery2, Set.of(objectName1, objectName2)),
          Arguments.of(beanQuery3, Set.of(objectName1, objectName2)),
          Arguments.of(beanQuery4, Set.of(objectName1, objectName2, objectName3, objectName4)));
    }

    @ParameterizedTest
    @MethodSource("dataset1")
    void canQuery(BeanQuery query, Set<ObjectName> expectedResult) {
      Set<BeanObject> expected =
          expectedResult.stream()
              .map(TestUtility.MBeansDataset::asBeanObject)
              .collect(Collectors.toSet());

      Set<BeanObject> sut = client.queryObject(query);

      assertEquals(expected, sut);
    }

    static Stream<Arguments> dataset2() {
      Map<String, String> props1 = Map.of("type", "object1");
      Map<String, Object> attrs1 = Map.of("Value", "String1");
      BeanObject beanObjectQuestion1 = new BeanObject("org.example", props1, Map.of("Value", ""));
      BeanObject beanObjectAnswer1 = new BeanObject("org.example", props1, attrs1);

      Map<String, String> props2 = Map.of("type", "object2");
      Map<String, Object> attrs2 = Map.of();
      BeanObject beanObjectQuestion2 = new BeanObject("org.example", props2, Map.of());
      BeanObject beanObjectAnswer2 = new BeanObject("org.example", props2, attrs2);

      Map<String, String> props3 = Map.of("type", "object3");
      Map<String, Object> attrs3 = Map.of("Value1", "String3", "Value2", 3);
      BeanObject beanObjectQuestion3 =
          new BeanObject("org.astraea", props3, Map.of("Value1", "", "Value2", 0));
      BeanObject beanObjectAnswer3 = new BeanObject("org.astraea", props3, attrs3);

      Map<String, String> props4 = Map.of("type", "object4");
      Map<String, Object> attrs4 = Map.of("Value1", "String4");
      BeanObject beanObjectQuestion4 = new BeanObject("org.astraea", props4, Map.of("Value1", ""));
      BeanObject beanObjectAnswer4 = new BeanObject("org.astraea", props4, attrs4);

      return Stream.of(
          Arguments.of(beanObjectQuestion1, beanObjectAnswer1),
          Arguments.of(beanObjectQuestion2, beanObjectAnswer2),
          Arguments.of(beanObjectQuestion3, beanObjectAnswer3),
          Arguments.of(beanObjectQuestion4, beanObjectAnswer4));
    }

    @ParameterizedTest
    @MethodSource("dataset2")
    void canFetch(BeanObject question, BeanObject answer) {

      BeanObject beanObject = client.fetchObjectAttribute(question);

      assertEquals(answer, beanObject);
    }
  }
}
