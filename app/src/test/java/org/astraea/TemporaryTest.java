package org.astraea;

import org.astraea.metrics.jmx.BeanQuery;
import org.junit.jupiter.api.Test;

public class TemporaryTest {

  @Test
  void testCanAccessBeanQueryBuilderFromOtherPackages() {
    BeanQuery.builder("com.example").property("key", "value").usePropertyListPattern().build();
  }
}
