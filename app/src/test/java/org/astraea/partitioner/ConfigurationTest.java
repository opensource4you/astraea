package org.astraea.partitioner;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {

  @Test
  void testString() {
    var config = Configuration.of(Map.of("key", "value"));
    Assertions.assertEquals("value", config.string("key"));
  }

  @Test
  void testList() {
    var config = Configuration.of(Map.of("key", "v0,v1"));
    Assertions.assertEquals(List.of("v0", "v1"), config.list("key", ","));
  }

  @Test
  void testMap() {
    var config = Configuration.of(Map.of("key", "v0:0,v1:1"));
    Assertions.assertEquals(
        Map.of("v0", 0, "v1", 1), config.map("key", ",", ":", Integer::valueOf));
  }
}
