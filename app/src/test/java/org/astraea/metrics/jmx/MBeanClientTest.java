package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.management.*;
import javax.management.remote.*;
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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory").build();

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
          BeanQuery.of("java.lang", Map.of("type", "MemoryManager", "name", "CodeCacheManager"))
              .build();
      BeanQuery query2 =
          BeanQuery.of("java.lang")
              .whereProperty("type", "MemoryManager")
              .whereProperty("name", "CodeCacheManager")
              .build();

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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory").build();
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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory").build();

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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "HelloWorld").build();

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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Memory").build();
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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "C*").build();

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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Something").build();

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
      BeanQuery beanQuery = BeanQuery.of("java.lang").whereProperty("type", "Something").build();

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
    BeanQuery query = BeanQuery.of("java.lang").whereProperty("type", "Memory").build();

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

  @Test
  void testGetAllMBeans() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {

      // act
      Set<BeanObject> beanObjects = client.queryBeans(BeanQuery.all());

      // assert
      assertTrue(beanObjects.stream().anyMatch(x -> x.domainName().equals("java.lang")));
      assertTrue(beanObjects.stream().anyMatch(x -> x.domainName().equals("java.nio")));
    }
  }

  @Test
  void testGetAllMBeansUnderSpecificDomainName() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {

      // act
      Set<BeanObject> beanObjects = client.queryBeans(BeanQuery.all("java.lang"));

      // assert
      assertTrue(beanObjects.size() > 1);
      assertTrue(beanObjects.stream().allMatch(x -> x.domainName().equals("java.lang")));
    }
  }

  @Test
  void testGetAllMBeansUnderSpecificDomainNamePattern() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {

      // act
      Set<BeanObject> beanObjects = client.queryBeans(BeanQuery.all("java.*"));

      // assert
      assertTrue(beanObjects.size() > 1);
      assertTrue(beanObjects.stream().allMatch(x -> x.domainName().matches("java.*")));
    }
  }

  @Test
  void testUsePropertyListPattern() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {
      BeanQuery patternQuery =
          BeanQuery.of("java.lang").whereProperty("type", "*").usePropertyListPattern().build();

      // act
      Set<BeanObject> beanObjects = client.queryBeans(patternQuery);

      // assert
      /*
      It might be hard to understand what this test is testing for.
      The keypoint is we are using BeanQueryBuilder#usePropertyListPattern()

      Without it the query will be "java.lang:type=*"
      And we only match the following
      java.lang:{type=OperatingSystem}
      java.lang:{type=Threading}
      java.lang:{type=ClassLoading}
      java.lang:{type=Compilation}
      java.lang:{type=Memory}
      java.lang:{type=Runtime}

      With it the query will be "java.lang:type=*,*"
      And we will match the following
      java.lang:{type=MemoryPool, name=CodeHeap 'non-nmethods'}
      java.lang:{type=GarbageCollector, name=G1 Young Generation}
      java.lang:{type=Runtime}
      java.lang:{type=OperatingSystem}
      java.lang:{type=Threading}
      java.lang:{type=MemoryPool, name=G1 Old Gen}
      java.lang:{type=MemoryPool, name=CodeHeap 'profiled nmethods'}
      java.lang:{type=MemoryPool, name=G1 Eden Space}
      java.lang:{type=MemoryPool, name=Metaspace}
      java.lang:{type=GarbageCollector, name=G1 Old Generation}
      java.lang:{type=Memory}
      java.lang:{type=MemoryPool, name=G1 Survivor Space}
      java.lang:{type=Compilation}
      java.lang:{type=MemoryManager, name=CodeCacheManager}
      java.lang:{type=MemoryPool, name=CodeHeap 'non-profiled nmethods'}
      java.lang:{type=MemoryManager, name=Metaspace Manager}
      java.lang:{type=ClassLoading}
      Notice how everything with "type" is match, even those with "name"
      */
      assertTrue(beanObjects.stream().anyMatch(x -> x.getProperties().containsKey("name")));
    }
  }

  @Test
  void testListDomains() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {

      // act
      List<String> domains = client.listDomains();

      // assert
      assertTrue(domains.contains("java.lang"));
      assertTrue(domains.contains("java.nio"));
    }
  }

  @Test
  void testGetAddress() throws Exception {
    // arrange
    try (MBeanClient client = new MBeanClient(jmxServer.getAddress())) {

      // act
      JMXServiceURL address = client.getAddress();

      // assert
      assertEquals(jmxServer.getAddress(), address);
    }
  }
}
