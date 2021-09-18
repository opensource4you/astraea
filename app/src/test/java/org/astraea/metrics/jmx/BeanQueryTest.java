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
            .whereProperty("name", "CodeCacheManager");

    assertEquals("java.lang", beanQuery.domainName());
  }

  @Test
  void propertyView() {
    BeanQuery beanQuery =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager");

    assertEquals(
        Map.of("type", "MemoryManager", "name", "CodeCacheManager"), beanQuery.propertyView());
  }

  @Test
  void objectName() throws MalformedObjectNameException {
    BeanQuery beanQuery =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager");

    assertEquals(
        ObjectName.getInstance("java.lang:type=MemoryManager,name=CodeCacheManager"),
        beanQuery.objectName());
  }

  @Test
  void whereProperty() {
    BeanQuery beanQuery0 =
        BeanQuery.of("java.lang")
            .whereProperty("type", "MemoryManager")
            .whereProperty("name", "CodeCacheManager");

    BeanQuery beanQuery1 = beanQuery0.whereProperty("hello", "world");

    assertTrue(beanQuery1.propertyView().containsKey("hello"));
    assertTrue(beanQuery1.propertyView().containsValue("world"));
    assertFalse(beanQuery0.propertyView().containsKey("hello"));
    assertFalse(beanQuery0.propertyView().containsValue("world"));
  }

  @Test
  void of() throws MalformedObjectNameException {
    // ObjectName version
    BeanQuery beanQueryFromObjectName =
        BeanQuery.of(ObjectName.getInstance("java.lang:type=Memory"));
    assertEquals("java.lang", beanQueryFromObjectName.domainName());
    assertEquals(Map.of("type", "Memory"), beanQueryFromObjectName.propertyView());

    // map version
    BeanQuery beanQueryFromMap = BeanQuery.of("java.lang", Map.of("type", "Memory"));
    assertEquals("java.lang", beanQueryFromMap.domainName());
    assertEquals(Map.of("type", "Memory"), beanQueryFromMap.propertyView());

    // HalfBakeBeanQuery
    //noinspection ConstantConditions
    assertFalse((Object) BeanQuery.of("java.lang") instanceof BeanQuery);
  }
}
