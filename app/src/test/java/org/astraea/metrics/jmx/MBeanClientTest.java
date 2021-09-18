package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MBeanClientTest {

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
  void test_Fetch_Attributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");

      // act
      BeanObject beanObject = sut.fetchAttributes(beanQuery);

      // assert
      assertTrue(beanObject.getPropertyKeySet().contains("type"));
      assertTrue(beanObject.getAttribute("HeapMemoryUsage").isPresent());
      assertTrue(beanObject.getAttribute("NonHeapMemoryUsage").isPresent());
    }
  }

  @Test
  void test_Fetch_Mbean_With_Multiple_Properties() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery query1 =
          BeanQuery.of("java.lang", Map.of("type", "MemoryManager", "name", "CodeCacheManager"));
      BeanQuery query2 =
          BeanQuery.of("java.lang")
              .whereProperty("type", "MemoryManager")
              .whereProperty("name", "CodeCacheManager");

      // act
      BeanObject beanObject1 = sut.fetchAttributes(query1);
      BeanObject beanObject2 = sut.fetchAttributes(query2);

      // assert
      assertTrue(beanObject1.getPropertyKeySet().contains("type"));
      assertTrue(beanObject1.getPropertyKeySet().contains("name"));
      assertTrue(beanObject1.getAttribute("MemoryPoolNames").isPresent());
      assertTrue(beanObject1.getAttribute("Name").isPresent());
      assertTrue(beanObject1.getAttribute("ObjectName").isPresent());
      assertTrue(beanObject1.getAttribute("Valid").isPresent());

      assertTrue(beanObject2.getPropertyKeySet().contains("type"));
      assertTrue(beanObject2.getPropertyKeySet().contains("name"));
      assertTrue(beanObject2.getAttribute("MemoryPoolNames").isPresent());
      assertTrue(beanObject2.getAttribute("Name").isPresent());
      assertTrue(beanObject2.getAttribute("ObjectName").isPresent());
      assertTrue(beanObject2.getAttribute("Valid").isPresent());
    }
  }

  @Test
  void test_Fetch_Selected_Attributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");
      String[] selectedAttribute = new String[] {"HeapMemoryUsage"};

      // act
      BeanObject beanObject = sut.fetchAttributes(beanQuery, selectedAttribute);

      // assert
      assertTrue(beanObject.getPropertyKeySet().contains("type"));
      assertTrue(beanObject.getAttribute("HeapMemoryUsage").isPresent());
      assertTrue(beanObject.getAttribute("NonHeapMemoryUsage").isEmpty());
    }
  }

  @Test
  void test_Try_Fetch_Attributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");

      // act
      Optional<BeanObject> beanObject = sut.tryFetchAttributes(beanQuery);

      // assert
      assertTrue(beanObject.isPresent());
      assertTrue(beanObject.get().getPropertyKeySet().contains("type"));
      assertTrue(beanObject.get().getAttribute("HeapMemoryUsage").isPresent());
      assertTrue(beanObject.get().getAttribute("NonHeapMemoryUsage").isPresent());
    }
  }

  @Test
  void test_Try_Non_Exists_MBean() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "HelloWorld");

      // act
      Optional<BeanObject> beanObject = sut.tryFetchAttributes(beanQuery);

      // assert
      assertTrue(beanObject.isEmpty());
    }
  }

  @Test
  void test_Try_Fetch_Selected_Attributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");
      String[] selectedAttribute = new String[] {"HeapMemoryUsage"};

      // act
      Optional<BeanObject> beanObject = sut.tryFetchAttributes(beanQuery, selectedAttribute);

      // assert
      assertTrue(beanObject.isPresent());
      assertTrue(beanObject.get().getPropertyKeySet().contains("type"));
      assertTrue(beanObject.get().getAttribute("HeapMemoryUsage").isPresent());
      assertTrue(beanObject.get().getAttribute("NonHeapMemoryUsage").isEmpty());
    }
  }

  @Test
  void test_Query_Beans() throws Exception {
    // arrange 1 query beans
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "C*");

      // act 1
      Set<BeanObject> beanObjects = sut.queryBeans(beanQuery);

      // assert 1
      assertEquals(2, beanObjects.size());
      assertTrue(
          beanObjects.stream().anyMatch(x -> x.getProperty("type").get().equals("ClassLoading")));
      assertTrue(
          beanObjects.stream().anyMatch(x -> x.getProperty("type").get().equals("Compilation")));

      // arrange 2 look into ClassLoading content

      // act
      Optional<BeanObject> classLoading =
          beanObjects.stream()
              .filter(x -> x.getPropertyView().get("type").equals("ClassLoading"))
              .findFirst();

      // assert
      assertTrue(classLoading.isPresent());
      assertEquals("java.lang", classLoading.get().domainName());
      assertEquals(5, classLoading.get().getAttributeView().size());
      assertTrue(classLoading.get().getAttribute("LoadedClassCount").isPresent());
      assertTrue(classLoading.get().getAttribute("ObjectName").isPresent());
      assertTrue(classLoading.get().getAttribute("TotalLoadedClassCount").isPresent());
      assertTrue(classLoading.get().getAttribute("UnloadedClassCount").isPresent());
      assertTrue(classLoading.get().getAttribute("Verbose").isPresent());
      assertTrue(classLoading.get().getAttribute("Verbose").get() instanceof Boolean);
    }
  }

  @Test
  void test_Query_Non_Exists_Beans() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Something");

      // act
      Set<BeanObject> beanObjects = sut.queryBeans(beanQuery);

      // assert
      assertEquals(0, beanObjects.size());
    }
  }

  @Test
  void test_Fetch_Non_Exists_Beans() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Something");

      // act assert
      assertThrows(
          InstanceNotFoundException.class,
          () -> {
            BeanObject beanObject = sut.fetchAttributes(beanQuery);
          });
      assertThrows(
          InstanceNotFoundException.class,
          () -> {
            BeanObject beanObject = sut.fetchAttributes(beanQuery, new String[0]);
          });
      assertDoesNotThrow(
          () -> {
            Optional<BeanObject> beanObject = sut.tryFetchAttributes(beanQuery);
            assertTrue(beanObject.isEmpty());
          });
      assertDoesNotThrow(
          () -> {
            Optional<BeanObject> beanObject = sut.tryFetchAttributes(beanQuery, new String[0]);
            assertTrue(beanObject.isEmpty());
          });
    }
  }
}
