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
package org.astraea.common.metrics;

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
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
    try (var client = JndiClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Memory").build();

      // act
      BeanObject beanObject = client.bean(beanQuery);

      // assert
      assertTrue(beanObject.properties().containsKey("type"));
      assertTrue(beanObject.attributes().containsKey("HeapMemoryUsage"));
      assertTrue(beanObject.attributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testFetchMbeanWithMultipleProperties() {
    // arrange
    try (var client = JndiClient.of(jmxServer.getAddress())) {
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
      BeanObject beanObject1 = client.bean(query1);
      BeanObject beanObject2 = client.bean(query2);

      // assert
      assertTrue(beanObject1.properties().containsKey("type"));
      assertTrue(beanObject1.properties().containsKey("name"));
      assertTrue(beanObject1.attributes().containsKey("MemoryPoolNames"));
      assertTrue(beanObject1.attributes().containsKey("Name"));
      assertTrue(beanObject1.attributes().containsKey("ObjectName"));
      assertTrue(beanObject1.attributes().containsKey("Valid"));

      assertTrue(beanObject2.properties().containsKey("type"));
      assertTrue(beanObject2.properties().containsKey("name"));
      assertTrue(beanObject2.attributes().containsKey("MemoryPoolNames"));
      assertTrue(beanObject2.attributes().containsKey("Name"));
      assertTrue(beanObject2.attributes().containsKey("ObjectName"));
      assertTrue(beanObject2.attributes().containsKey("Valid"));
    }
  }

  @Test
  void testFetchSelectedAttributes()
      throws ReflectionException,
          InstanceNotFoundException,
          IOException,
          AttributeNotFoundException,
          MBeanException {
    // arrange
    try (var client = (JndiClient.BasicMBeanClient) JndiClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Memory").build();
      List<String> selectedAttribute = List.of("HeapMemoryUsage");

      // act
      BeanObject beanObject = client.queryBean(beanQuery, selectedAttribute);

      // assert
      assertTrue(beanObject.properties().containsKey("type"));
      assertTrue(beanObject.attributes().containsKey("HeapMemoryUsage"));
      assertFalse(beanObject.attributes().containsKey("NonHeapMemoryUsage"));
    }
  }

  @Test
  void testQueryBeans() {
    // arrange 1 query beans
    try (var client = JndiClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "C*").build();

      // act 1
      Collection<BeanObject> beanObjects = client.beans(beanQuery);

      // assert 1
      assertEquals(2, beanObjects.size());
      assertTrue(
          beanObjects.stream().anyMatch(x -> x.properties().get("type").equals("ClassLoading")));
      assertTrue(
          beanObjects.stream().anyMatch(x -> x.properties().get("type").equals("Compilation")));

      // arrange 2 look into ClassLoading content

      // act
      Optional<BeanObject> classLoading =
          beanObjects.stream()
              .filter(x -> x.properties().get("type").equals("ClassLoading"))
              .findFirst();

      // assert
      assertTrue(classLoading.isPresent());
      assertEquals("java.lang", classLoading.get().domainName());
      assertEquals(5, classLoading.get().attributes().size());
      assertTrue(classLoading.get().attributes().containsKey("LoadedClassCount"));
      assertTrue(classLoading.get().attributes().containsKey("ObjectName"));
      assertTrue(classLoading.get().attributes().containsKey("TotalLoadedClassCount"));
      assertTrue(classLoading.get().attributes().containsKey("UnloadedClassCount"));
      assertTrue(classLoading.get().attributes().containsKey("Verbose"));
      assertTrue(classLoading.get().attributes().get("Verbose") instanceof Boolean);
    }
  }

  @Test
  void testQueryNonExistsBeans() {
    // arrange
    try (var client = JndiClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Something").build();

      // act
      Collection<BeanObject> beanObjects = client.beans(beanQuery);

      // assert
      assertEquals(0, beanObjects.size());
    }
  }

  @Test
  void testFetchNonExistsBeans() {
    // arrange
    try (var client = (JndiClient.BasicMBeanClient) JndiClient.of(jmxServer.getAddress())) {
      BeanQuery beanQuery =
          BeanQuery.builder().domainName("java.lang").property("type", "Something").build();

      // act assert
      assertThrows(NoSuchElementException.class, () -> client.bean(beanQuery));
      assertThrows(
          javax.management.InstanceNotFoundException.class,
          () -> client.queryBean(beanQuery, Collections.emptyList()));
    }
  }

  @Test
  void testCloseOnceMore() {
    // arrange
    var client = JndiClient.of(jmxServer.getAddress());

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
    try (var client = JndiClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> beanObjects = client.beans(BeanQuery.all());

      // assert
      assertTrue(beanObjects.stream().anyMatch(x -> x.domainName().equals("java.lang")));
      assertTrue(beanObjects.stream().anyMatch(x -> x.domainName().equals("java.nio")));
    }
  }

  @Test
  void testGetAllMBeansUnderSpecificDomainName() {
    // arrange
    try (var client = JndiClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> beanObjects = client.beans(BeanQuery.all("java.lang"));

      // assert
      assertTrue(beanObjects.size() > 1);
      assertTrue(beanObjects.stream().allMatch(x -> x.domainName().equals("java.lang")));
    }
  }

  @Test
  void testGetAllMBeansUnderSpecificDomainNamePattern() {
    // arrange
    try (var client = JndiClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> beanObjects = client.beans(BeanQuery.all("java.*"));

      // assert
      assertTrue(beanObjects.size() > 1);
      assertTrue(beanObjects.stream().allMatch(x -> x.domainName().matches("java.*")));
    }
  }

  @Test
  void testUsePropertyListPatternForRemote() {
    testUsePropertyListPattern(JndiClient.of(jmxServer.getAddress()));
  }

  @Test
  void testUsePropertyListPatternForLocal() {
    testUsePropertyListPattern(JndiClient.local());
  }

  private void testUsePropertyListPattern(JndiClient client) {
    // arrange
    try (client) {
      BeanQuery patternQuery =
          BeanQuery.builder()
              .domainName("java.lang")
              .property("type", "*")
              .propertyListPattern(true)
              .build();

      // act
      Collection<BeanObject> beanObjects = client.beans(patternQuery);

      // assert
      /*
      It might be hard to understand what this test is testing for.
      The keypoint is we are using BeanQueryBuilder#propertyListPattern(true)

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
      assertTrue(beanObjects.stream().anyMatch(x -> x.properties().containsKey("name")));
    }
  }

  @Test
  void testListDomains() {
    // arrange
    try (var client = (JndiClient.BasicMBeanClient) JndiClient.of(jmxServer.getAddress())) {

      // act
      List<String> domains = client.domains();

      // assert
      assertTrue(domains.contains("java.lang"));
      assertTrue(domains.contains("java.nio"));
    }
  }

  @Test
  void testHostAndPort() {
    // arrange
    try (var client = (JndiClient.BasicMBeanClient) JndiClient.of(jmxServer.getAddress())) {
      assertEquals(jmxServer.getAddress().getHost(), client.host);
      assertEquals(jmxServer.getAddress().getPort(), client.port);
    }
  }

  @Test
  void testCustomMBeanWith500Attributes() throws Exception {
    // arrange
    Map<String, Integer> mbeanPayload =
        IntStream.range(0, 500).boxed().collect(toMap(i -> "attribute" + i, i -> i));
    Object customMBean0 = createReadOnlyDynamicMBean(mbeanPayload);
    ObjectName objectName0 = ObjectName.getInstance("com.example:type=test0");

    register(objectName0, customMBean0);

    try (var client = JndiClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> all =
          client.beans(
              BeanQuery.builder().domainName("com.example").property("type", "test*").build());

      // assert
      Map<String, Object> mergedCollect =
          all.stream()
              .flatMap(beanObject -> beanObject.attributes().entrySet().stream())
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
      Object mbean = createReadOnlyDynamicMBean(Map.of("attribute", i));
      register(objectName, mbean);
    }

    try (var client = JndiClient.of(jmxServer.getAddress())) {

      // act
      Collection<BeanObject> all =
          client.beans(
              BeanQuery.builder().domainName("com.example").property("type", "test*").build());

      // assert
      List<Map.Entry<String, String>> properties =
          all.stream()
              .map(BeanObject::properties)
              .map(Map::entrySet)
              .flatMap(Collection::stream)
              .collect(toList());
      Set<String> propKeys = properties.stream().map(Map.Entry::getKey).collect(toSet());
      Set<String> propValues = properties.stream().map(Map.Entry::getValue).collect(toSet());

      List<Map.Entry<String, Object>> attributes =
          all.stream()
              .map(BeanObject::attributes)
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

  @Test
  void testLocal() {
    var client = JndiClient.local();
    Assertions.assertNotEquals(0, client.beans(BeanQuery.all()).size());
  }

  static class DynamicMBean implements javax.management.DynamicMBean {

    private final MBeanInfo mBeanInfo;
    private final Map<String, ?> readonlyAttributes;

    DynamicMBean(Map<String, ?> readonlyAttributes) {
      this.readonlyAttributes = readonlyAttributes;
      var attributeInfos =
          readonlyAttributes.entrySet().stream()
              .map(
                  entry ->
                      new MBeanAttributeInfo(
                          entry.getKey(),
                          entry.getValue().getClass().getName(),
                          "",
                          true,
                          false,
                          false))
              .toArray(MBeanAttributeInfo[]::new);
      this.mBeanInfo =
          new MBeanInfo(
              DynamicMBean.class.getName(),
              "Readonly Dynamic MBean for Testing Purpose",
              attributeInfos,
              new MBeanConstructorInfo[0],
              new MBeanOperationInfo[0],
              new MBeanNotificationInfo[0]);
    }

    @Override
    public Object getAttribute(String attributeName) throws AttributeNotFoundException {
      if (readonlyAttributes.containsKey(attributeName))
        return readonlyAttributes.get(attributeName);

      throw new AttributeNotFoundException();
    }

    @Override
    public void setAttribute(Attribute attribute) {
      throw new RuntimeOperationsException(new IllegalArgumentException("readonly"));
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
      final AttributeList list = new AttributeList();
      for (String attribute : attributes) {
        if (readonlyAttributes.containsKey(attribute))
          list.add(new Attribute(attribute, readonlyAttributes.get(attribute)));
      }

      return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
      throw new RuntimeOperationsException(new IllegalArgumentException("readonly"));
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MBeanInfo getMBeanInfo() {
      return mBeanInfo;
    }
  }

  /**
   * Create a readonly dynamic MBeans
   *
   * @param attributes for each key/value pair. the key string represent the attribute name, and the
   *     value represent the attribute value
   */
  public static DynamicMBean createReadOnlyDynamicMBean(Map<String, ?> attributes) {
    return new DynamicMBean(attributes);
  }
}
