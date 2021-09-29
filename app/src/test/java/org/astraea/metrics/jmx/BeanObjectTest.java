package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class BeanObjectTest {

  BeanObject createBeanObject() {
    return new BeanObject(
        "java.lang", Map.of("type", "Memory"), Map.of("HeapMemoryUsage", "content"));
  }

  @Test
  void domainName() {
    BeanObject beanObject = createBeanObject();

    String domainName = beanObject.domainName();

    assertEquals("java.lang", domainName);
  }

  @Test
  void getPropertyView() {
    BeanObject beanObject = createBeanObject();

    Map<String, String> propertyView = beanObject.getProperties();

    assertEquals(Map.of("type", "Memory"), propertyView);
  }

  @Test
  void getAttributeView() {
    BeanObject beanObject = createBeanObject();

    Map<String, Object> result = beanObject.getAttributes();

    assertEquals(Map.of("HeapMemoryUsage", "content"), result);
  }

  @Test
  void testToString() {
    BeanObject beanObject = createBeanObject();

    String s = beanObject.toString();

    assertTrue(s.contains(beanObject.domainName()));
  }
}
