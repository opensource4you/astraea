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

  @Test
  void testMBeanClientFetch() throws Exception {
    // arrange
    final ObjectName fetchMe = ObjectName.getInstance("org.example", "type", "object1");
    final ObjectName objectName2 = ObjectName.getInstance("org.example", "type", "object2");
    final ObjectName objectName3 = ObjectName.getInstance("org.astraea", "type", "object3");
    final ObjectName objectName4 = ObjectName.getInstance("org.astraea", "type", "object4");

    final Map<ObjectName, Object> mbeansServerContent =
        Map.of(
            fetchMe, new TestUtility.OneAttribute<String>("FetchMe"),
            objectName2, new TestUtility.OneAttribute<String>("value2"),
            objectName3, new TestUtility.TwoAttribute<String, Integer>("value3", 3),
            objectName4, new TestUtility.TwoAttribute<String, Integer>("value4", 4));

    JMXConnectorServer jmxConnectorServer = TestUtility.setupJmxServerSpy(mbeansServerContent);
    jmxConnectorServer.start();
    MBeanClient client = new MBeanClient(jmxConnectorServer.getAddress());
    client.connect();

    // act
    BeanObject fetchResult =
        client.fetchObjectAttribute(
            BeanObject.fromDomainName("org.example")
                .selectProperty("type", "object1")
                .fetchAttribute("Value"));

    // assert
    assertEquals(fetchMe.getKeyPropertyList(), fetchResult.getPropertyView());
    assertEquals("FetchMe", fetchResult.getAttributeView().get("Value"));

    client.close();
  }

  @Test
  void testMBeanClientQueryAndFetch() throws Exception {
    // arrange 1
    final ObjectName target1 = ObjectName.getInstance("org.example", "type", "object1");
    final ObjectName target2 = ObjectName.getInstance("org.example", "type", "object2");
    final ObjectName target3 = ObjectName.getInstance("org.astraea", "type", "object3");
    final ObjectName target4 = ObjectName.getInstance("org.astraea", "type", "object4");

    final Map<ObjectName, Object> mbeansServerContent =
        Map.of(
            target1, new TestUtility.OneAttribute<String>("value1"),
            target2, new TestUtility.OneAttribute<String>("value2"),
            target3, new TestUtility.TwoAttribute<String, Integer>("value3", 3),
            target4, new TestUtility.TwoAttribute<String, Integer>("value4", 4));

    JMXConnectorServer jmxConnectorServer = TestUtility.setupJmxServerSpy(mbeansServerContent);
    jmxConnectorServer.start();
    MBeanClient client = new MBeanClient(jmxConnectorServer.getAddress());
    client.connect();

    final BeanObject expectResult1 =
        BeanObject.fromDomainName("org.example").selectProperty("type", "object1");
    final BeanObject expectResult2 =
        BeanObject.fromDomainName("org.example").selectProperty("type", "object2");
    final BeanObject expectResult3 =
        BeanObject.fromDomainName("org.astraea").selectProperty("type", "object3");
    final BeanObject expectResult4 =
        BeanObject.fromDomainName("org.astraea").selectProperty("type", "object4");

    // act 1
    Set<BeanObject> queryResult1 =
        client.queryObject(BeanQuery.forDomainName("org.*").whereProperty("type", "object?"));

    // assert 1
    assertEquals(4, queryResult1.size());
    assertTrue(queryResult1.contains(expectResult1));
    assertTrue(queryResult1.contains(expectResult2));
    assertTrue(queryResult1.contains(expectResult3));
    assertTrue(queryResult1.contains(expectResult4));

    // arrange 2 Query & Fetch Attributes
    BeanObject expectedAnswer =
        new BeanObject("org.example", Map.of("type", "object1"), Map.of("Value", "value1"));

    // act 2 query
    Set<BeanObject> query =
        client.queryObject(BeanQuery.forDomainName("org.example").whereProperty("type", "object1"));
    BeanObject queryResult2 = query.stream().findFirst().get();
    // act 2 update fetch target's attribute
    BeanObject fetchTarget = queryResult2.fetchAttribute("Value");
    // act 2 fetch object
    BeanObject fetchResult = client.fetchObjectAttribute(fetchTarget);

    // assert 2
    assertEquals(1, query.size());
    assertEquals(expectedAnswer, fetchResult);

    client.close();
  }
}
