package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

class BeanQueryTest {

  @Test
  void domainName() {
    BeanQuery beanQuery =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager")
            .build();

    assertEquals("java.lang", beanQuery.domainName());
  }

  @Test
  void propertyView() {
    BeanQuery beanQuery =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager")
            .build();

    assertEquals(
        Map.of("type", "MemoryManager", "name", "CodeCacheManager"), beanQuery.properties());
  }

  @Test
  void objectName() throws MalformedObjectNameException {
    BeanQuery beanQuery =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager")
            .build();

    assertEquals(
        ObjectName.getInstance("java.lang:type=MemoryManager,name=CodeCacheManager"),
        beanQuery.getQuery());
  }

  @Test
  void whereProperty() {
    BeanQuery.BeanQueryBuilder beanQueryBuilder =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager");

    BeanQuery beanQuery0 = beanQueryBuilder.build();
    BeanQuery beanQuery1 = beanQueryBuilder.whereProperty("hello", "world").build();

    assertTrue(beanQuery1.properties().containsKey("hello"));
    assertTrue(beanQuery1.properties().containsValue("world"));
    assertFalse(beanQuery0.properties().containsKey("hello"));
    assertFalse(beanQuery0.properties().containsValue("world"));
  }

  @Test
  void of() throws MalformedObjectNameException {
    // ObjectName version
    BeanQuery beanQueryFromObjectName =
        BeanQuery.fromObjectName(ObjectName.getInstance("java.lang:type=Memory"));
    assertEquals("java.lang", beanQueryFromObjectName.domainName());
    assertEquals(Map.of("type", "Memory"), beanQueryFromObjectName.properties());

    // map version
    BeanQuery beanQueryFromMap = BeanQuery.of("java.lang", Map.of("type", "Memory")).build();
    assertEquals("java.lang", beanQueryFromMap.domainName());
    assertEquals(Map.of("type", "Memory"), beanQueryFromMap.properties());

    // all under specific domain
    BeanQuery beanQueryForDomain = BeanQuery.all("java.lang");
    assertEquals(0, beanQueryForDomain.getQuery().getKeyPropertyList().size());
    assertEquals("java.lang", beanQueryForDomain.getQuery().getDomain());

    // all in JMX
    BeanQuery beanQueryForAllDomain = BeanQuery.all();
    assertEquals(0, beanQueryForAllDomain.getQuery().getKeyPropertyList().size());
    assertEquals("*", beanQueryForAllDomain.getQuery().getDomain());

    // usePropertyListPattern
    BeanQuery withPattern =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .usePropertyListPattern()
            .build();
    assertTrue(withPattern.getQuery().isPropertyListPattern());

    // invalid query: query whole domain name without specify "property list pattern"
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          BeanQuery.of("java.lang").build();
        });
  }
}
