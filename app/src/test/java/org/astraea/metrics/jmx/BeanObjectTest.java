package org.astraea.metrics.jmx;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BeanObjectTest {

  @Test
  void constructorShouldMakeDefensiveCopy() {
    Map<String, String> properties = new HashMap<>();
    Map<String, Object> attributes = new HashMap<>();

    // construct the bean object with two given map
    BeanObject beanObject = new BeanObject("example.com", properties, attributes);

    // modify the maps
    properties.put("key", "value");
    attributes.put("key", "value");
    // if the constructor have make defensive copy, the original given map won't affect their value
    assertTrue(beanObject.getPropertyView().isEmpty());
    assertTrue(beanObject.getAttributeView().isEmpty());
  }
}
