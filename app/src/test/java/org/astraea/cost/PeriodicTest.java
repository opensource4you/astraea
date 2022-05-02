package org.astraea.cost;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PeriodicTest extends Periodic<Map<Integer, Double>> {
  private double brokerValue = 0.0;

  @Test
  void testTryUpdate() {
    var broker1 = tryUpdate(this::testMap, 1);
    Assertions.assertEquals(broker1.get(0), 0.0);
    var broker2 = tryUpdate(this::testMap, 1);
    Assertions.assertEquals(broker2.get(0), 0.0);
    sleep(1);
    broker2 = tryUpdate(this::testMap, 1);
    Assertions.assertEquals(broker2.get(0), 1.0);
  }

  Map<Integer, Double> testMap() {
    var broker = new HashMap<Integer, Double>();
    broker.put(0, brokerValue);
    brokerValue++;
    return broker;
  }

  private static void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
