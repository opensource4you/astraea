/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.metrics.jmx;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MBeanClientTest {

  private MBeanServer mBeanServer;
  private JMXConnectorServer jmxServer;
  private final Map<ObjectName, Object> registeredBeans = new HashMap<>();

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
  }

  @AfterEach
  void tearDown() throws IOException {
    jmxServer.stop();
    clearRegisteredMBeans();
    mBeanServer = null;
  }

  @Test
  void testFetchAttributes() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Memory").build();

      // act
      BeanObject beanObject = client.queryBean(beanQuery);

      // assert
      assertTrue(beanObject.getProperties().containsKey("type"));
      assertTrue(beanObject.getAttributes().containsKey("HeapMemoryUsage"));
      assertTrue(beanObject.getAttributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testFetchMbeanWithMultipleProperties() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery query1 =
          BeanQuery.builder()
              .domainName("java.lang")
              .properties(Map.of("type", "MemoryManager", "name", "CodeCacheManager"))
              .build();
      BeanQuery query2 =
          BeanQuery.builder()
              .domainName("java.lang")
              .property("type", "MemoryManager")
              .property("name", "CodeCacheManager")
              .build();

      // act
      BeanObject beanObject1 = client.queryBean(query1);
      BeanObject beanObject2 = client.queryBean(query2);

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
  void testFetchSelectedAttributes() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Memory").build();
      List<String> selectedAttribute = List.of("HeapMemoryUsage");

      // act
      BeanObject beanObject = client.queryBean(beanQuery, selectedAttribute);

      // assert
      assertTrue(beanObject.getProperties().containsKey("type"));
      assertTrue(beanObject.getAttributes().containsKey("HeapMemoryUsage"));
      assertFalse(beanObject.getAttributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testQueryBeans() {
    // arrange 1 query beans
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "C*").build();

      // act 1
      Collection<BeanObject> beanObjects = client.queryBeans(beanQuery);

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
  void testQueryNonExistsBeans() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Something").build();

      // act
      Collection<BeanObject> beanObjects = client.queryBeans(beanQuery);

      // assert
      assertEquals(0, beanObjects.size());
    }
  }

  @Test
  void testFetchNonExistsBeans() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Something").build();

      // act assert
      assertThrows(NoSuchElementException.class, () -> client.queryBean(beanQuery));
      assertThrows(
          NoSuchElementException.class, () -> client.queryBean(beanQuery, Collections.emptyList()));
    }
  }

  @Test
  void testUseClosedClientWillThrowError() {
    // arrange
    var client = MBeanClient.of(jmxServer.getAddress());
    BeanQuery query =
        BeanQuery.builder().domainName("java.lang").property("type", "Memory").build();

    // act
    client.close();

    // assert
    assertThrows(IllegalStateException.class, () -> client.queryBean(query));
    assertThrows(
        IllegalStateException.class, () -> client.queryBean(query, Collections.emptyList()));
    assertThrows(IllegalStateException.class, () -> client.queryBeans(query));
  }

  @Test
  void testCloseOnceMore() {
    // arrange
    var client = MBeanClient.of(jmxServer.getAddress());

    // act
    client.close();

    // assert
    client.close();
    client.close();
    client.close();
    client.close();
    client.close();
  }

  @Test
  void testGetAllMBeans() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> beanObjects = client.queryBeans(BeanQuery.all());

      // assert
      assertTrue(beanObjects.stream().anyMatch(x -> x.domainName().equals("java.lang")));
      assertTrue(beanObjects.stream().anyMatch(x -> x.domainName().equals("java.nio")));
    }
  }

  @Test
  void testGetAllMBeansUnderSpecificDomainName() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> beanObjects = client.queryBeans(BeanQuery.all("java.lang"));

      // assert
      assertTrue(beanObjects.size() > 1);
      assertTrue(beanObjects.stream().allMatch(x -> x.domainName().equals("java.lang")));
    }
  }

  @Test
  void testGetAllMBeansUnderSpecificDomainNamePattern() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> beanObjects = client.queryBeans(BeanQuery.all("java.*"));

      // assert
      assertTrue(beanObjects.size() > 1);
      assertTrue(beanObjects.stream().allMatch(x -> x.domainName().matches("java.*")));
    }
  }

  @Test
  void testUsePropertyListPattern() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {
      BeanQuery patternQuery =
          BeanQuery.builder()
              .domainName("java.lang")
              .property("type", "*")
              .usePropertyListPattern()
              .build();

      // act
      Collection<BeanObject> beanObjects = client.queryBeans(patternQuery);

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
  void testListDomains() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      // act
      List<String> domains = client.listDomains();

      // assert
      assertTrue(domains.contains("java.lang"));
      assertTrue(domains.contains("java.nio"));
    }
  }

  @Test
  void testHostAndPort() {
    // arrange
    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      assertEquals(jmxServer.getAddress().getHost(), client.host());
      assertEquals(jmxServer.getAddress().getPort(), client.port());
    }
  }

  @Test
  void testCustomMBeanWith500Attributes() throws Exception {
    // arrange
    Map<String, Integer> mbeanPayload =
        IntStream.range(0, 500).boxed().collect(toMap(i -> "attribute" + i, i -> i));
    Object customMBean0 = Utility.createReadOnlyDynamicMBean(mbeanPayload);
    ObjectName objectName0 = ObjectName.getInstance("com.example:type=test0");

    register(objectName0, customMBean0);

    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> all =
          client.queryBeans(
              BeanQuery.builder().domainName("com.example").property("type", "test*").build());

      // assert
      Map<String, Object> mergedCollect =
          all.stream()
              .flatMap(beanObject -> beanObject.getAttributes().entrySet().stream())
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

      assertEquals(500, mergedCollect.size());
      assertEquals(mbeanPayload, mergedCollect);
    }
  }

  @Test
  void testWith100CustomMBeans() throws Exception {
    // arrange
    for (int i = 0; i < 100; i++) {
      ObjectName objectName = ObjectName.getInstance("com.example:type=test" + i);
      Object mbean = Utility.createReadOnlyDynamicMBean(Map.of("attribute", i));
      register(objectName, mbean);
    }

    try (var client = MBeanClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> all =
          client.queryBeans(
              BeanQuery.builder().domainName("com.example").property("type", "test*").build());

      // assert
      List<Map.Entry<String, String>> properties =
          all.stream()
              .map(BeanObject::getProperties)
              .map(Map::entrySet)
              .flatMap(Collection::stream)
              .collect(toList());
      Set<String> propKeys = properties.stream().map(Map.Entry::getKey).collect(toSet());
      Set<String> propValues = properties.stream().map(Map.Entry::getValue).collect(toSet());

      List<Map.Entry<String, Object>> attributes =
          all.stream()
              .map(BeanObject::getAttributes)
              .map(Map::entrySet)
              .flatMap(Collection::stream)
              .collect(toList());
      Set<String> attrKeys = attributes.stream().map(Map.Entry::getKey).collect(toSet());
      Set<Object> attrValues = attributes.stream().map(Map.Entry::getValue).collect(toSet());

      assertEquals(100, all.size());
      assertEquals(100, properties.size());
      assertEquals(100, attributes.size());
      assertEquals(Set.of("type"), propKeys);
      assertEquals(Set.of("attribute"), attrKeys);
      assertEquals(IntStream.range(0, 100).mapToObj(x -> "test" + x).collect(toSet()), propValues);
      assertEquals(IntStream.range(0, 100).boxed().collect(toSet()), attrValues);
    }
  }
}
