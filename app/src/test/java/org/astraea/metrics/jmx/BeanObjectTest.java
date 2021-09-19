package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Set;
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
  void getAttributeKeySet() {
    BeanObject beanObject = createBeanObject();

    Set<String> result = beanObject.getAttributeKeySet();

    assertEquals(Set.of("HeapMemoryUsage"), result);
  }

  @Test
  void getPropertyKeySet() {
    BeanObject beanObject = createBeanObject();

    Set<String> result = beanObject.getPropertyKeySet();

    assertEquals(Set.of("type"), result);
  }

  @Test
  void getAttribute() {
    BeanObject beanObject = createBeanObject();

    boolean a1 = beanObject.getAttribute("HeapMemoryUsage").isPresent();
    boolean a2 = beanObject.getAttribute("NotExists").isEmpty();
    Object a3 = beanObject.getAttribute("HeapMemoryUsage").get();
    Object a4 = beanObject.getAttribute("NotExists", "DefaultValue");
    Object a5 = beanObject.getAttribute("NotExists", () -> "HeavyCalc");

    assertTrue(a1);
    assertTrue(a2);
    assertEquals("content", a3);
    assertEquals("DefaultValue", a4);
    assertEquals("HeavyCalc", a5);
  }

  @Test
  void getProperty() {
    BeanObject beanObject = createBeanObject();

    boolean a1 = beanObject.getProperty("type").isPresent();
    boolean a2 = beanObject.getProperty("NotExists").isEmpty();
    String a3 = beanObject.getProperty("type").get();
    String a4 = beanObject.getProperty("NotExists", "DefaultValue");
    Object a5 = beanObject.getProperty("NotExists", () -> "HeavyCalc");

    assertTrue(a1);
    assertTrue(a2);
    assertEquals("Memory", a3);
    assertEquals("DefaultValue", a4);
    assertEquals("HeavyCalc", a5);
  }
}
