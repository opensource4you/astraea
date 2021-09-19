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
  void testFetchAttributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");

      // act
      BeanObject beanObject = sut.queryBean(beanQuery);

      // assert
      assertTrue(beanObject.getProperties().containsKey("type"));
      assertTrue(beanObject.getAttributes().containsKey("HeapMemoryUsage"));
      assertTrue(beanObject.getAttributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testFetchMbeanWithMultipleProperties() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery query1 =
          BeanQuery.of("java.lang", Map.of("type", "MemoryManager", "name", "CodeCacheManager"));
      BeanQuery query2 =
          BeanQuery.of("java.lang")
              .whereProperty("type", "MemoryManager")
              .whereProperty("name", "CodeCacheManager");

      // act
      BeanObject beanObject1 = sut.queryBean(query1);
      BeanObject beanObject2 = sut.queryBean(query2);

      // assert
      assertTrue(beanObject1.getProperties().containsKey("type"));
      assertTrue(beanObject1.getProperties().containsKey("name"));
      assertTrue(beanObject1.getAttributes().containsKey("MemoryPoolNames"));
      assertTrue(beanObject1.getAttributes().containsKey("Name"));
      assertTrue(beanObject1.getAttributes().containsKey("ObjectName"));
      assertTrue(beanObject1.getAttributes().containsKey("Valid"));

      assertTrue(beanObject2.getProperties().containsKey("type"));
      assertTrue(beanObject2.getProperties().containsKey("name"));
      assertTrue(beanObject2.getAttributes().containsKey("MemoryPoolNames"));
      assertTrue(beanObject2.getAttributes().containsKey("Name"));
      assertTrue(beanObject2.getAttributes().containsKey("ObjectName"));
      assertTrue(beanObject2.getAttributes().containsKey("Valid"));
    }
  }

  @Test
  void testFetchSelectedAttributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");
      String[] selectedAttribute = new String[] {"HeapMemoryUsage"};

      // act
      BeanObject beanObject = sut.queryBean(beanQuery, selectedAttribute);

      // assert
      assertTrue(beanObject.getProperties().containsKey("type"));
      assertTrue(beanObject.getAttributes().containsKey("HeapMemoryUsage"));
      assertFalse(beanObject.getAttributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testTryFetchAttributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");

      // act
      Optional<BeanObject> beanObject = sut.tryQueryBean(beanQuery);

      // assert
      assertTrue(beanObject.isPresent());
      assertTrue(beanObject.get().getProperties().containsKey("type"));
      assertTrue(beanObject.get().getAttributes().containsKey("HeapMemoryUsage"));
      assertTrue(beanObject.get().getAttributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testTryFetchNonExistsMBean() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "HelloWorld");

      // act
      Optional<BeanObject> beanObject = sut.tryQueryBean(beanQuery);

      // assert
      assertTrue(beanObject.isEmpty());
    }
  }

  @Test
  void testTryFetchSelectedAttributes() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory");
      String[] selectedAttribute = new String[] {"HeapMemoryUsage"};

      // act
      Optional<BeanObject> beanObject = sut.tryQueryBean(beanQuery, selectedAttribute);

      // assert
      assertTrue(beanObject.isPresent());
      assertTrue(beanObject.get().getProperties().containsKey("type"));
      assertTrue(beanObject.get().getAttributes().containsKey("HeapMemoryUsage"));
      assertFalse(beanObject.get().getAttributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testQueryBeans() throws Exception {
    // arrange 1 query beans
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "C*");

      // act 1
      Set<BeanObject> beanObjects = sut.queryBeans(beanQuery);

      // assert 1
      assertEquals(2, beanObjects.size());
      assertTrue(
          beanObjects.stream().anyMatch(x -> x.getProperties().get("type").equals("ClassLoading")));
      assertTrue(
          beanObjects.stream().anyMatch(x -> x.getProperties().get("type").equals("Compilation")));

      // arrange 2 look into ClassLoading content

      // act
      Optional<BeanObject> classLoading =
          beanObjects.stream()
              .filter(x -> x.getProperties().get("type").equals("ClassLoading"))
              .findFirst();

      // assert
      assertTrue(classLoading.isPresent());
      assertEquals("java.lang", classLoading.get().domainName());
      assertEquals(5, classLoading.get().getAttributes().size());
      assertTrue(classLoading.get().getAttributes().containsKey("LoadedClassCount"));
      assertTrue(classLoading.get().getAttributes().containsKey("ObjectName"));
      assertTrue(classLoading.get().getAttributes().containsKey("TotalLoadedClassCount"));
      assertTrue(classLoading.get().getAttributes().containsKey("UnloadedClassCount"));
      assertTrue(classLoading.get().getAttributes().containsKey("Verbose"));
      assertTrue(classLoading.get().getAttributes().get("Verbose") instanceof Boolean);
    }
  }

  @Test
  void testQueryNonExistsBeans() throws Exception {
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
  void testFetchNonExistsBeans() throws Exception {
    // arrange
    try (MBeanClient sut = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Something");

      // act assert
      assertThrows(
          InstanceNotFoundException.class,
          () -> {
            BeanObject beanObject = sut.queryBean(beanQuery);
          });
      assertThrows(
          InstanceNotFoundException.class,
          () -> {
            BeanObject beanObject = sut.queryBean(beanQuery, new String[0]);
          });
      assertDoesNotThrow(
          () -> {
            Optional<BeanObject> beanObject = sut.tryQueryBean(beanQuery);
            assertTrue(beanObject.isEmpty());
          });
      assertDoesNotThrow(
          () -> {
            Optional<BeanObject> beanObject = sut.tryQueryBean(beanQuery, new String[0]);
            assertTrue(beanObject.isEmpty());
          });
    }
  }

  @Test
  void testUseClosedClientWillThrowError() throws Exception {
    // arrange
    MBeanClient sut = new MBeanClient(jmxServer.getAddress());
    BeanQuery query = BeanQuery.of("java.lang").whereProperty("type", "Memory");

    // act
    sut.close();

    // assert
    assertThrows(IllegalStateException.class, () -> sut.queryBean(query));
    assertThrows(IllegalStateException.class, () -> sut.queryBean(query, new String[0]));
    assertThrows(IllegalStateException.class, () -> sut.tryQueryBean(query));
    assertThrows(IllegalStateException.class, () -> sut.tryQueryBean(query, new String[0]));
    assertThrows(IllegalStateException.class, () -> sut.queryBeans(query));
  }

  @Test
  void testCloseOnceMoreWillThrowError() throws Exception {
    // arrange
    MBeanClient sut = new MBeanClient(jmxServer.getAddress());

    // act
    sut.close();

    // assert
    assertThrows(IllegalStateException.class, sut::close);
    assertThrows(IllegalStateException.class, sut::close);
    assertThrows(IllegalStateException.class, sut::close);
    assertThrows(IllegalStateException.class, sut::close);
    assertThrows(IllegalStateException.class, sut::close);
  }
}
