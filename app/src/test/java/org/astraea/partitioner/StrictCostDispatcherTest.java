package org.astraea.partitioner;

import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StrictCostDispatcherTest {

  @Test
  void testClose() {
    var dispatcher = new StrictCostDispatcher();
    dispatcher.close();
  }

  @Test
  void testConfigure() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of(Map.of()));
      Assertions.assertThrows(NoSuchElementException.class, () -> dispatcher.jmxPort(0));
      dispatcher.configure(Configuration.of(Map.of(StrictCostDispatcher.JMX_PORT, "12345")));
      Assertions.assertEquals(12345, dispatcher.jmxPort(0));
    }
  }
}
