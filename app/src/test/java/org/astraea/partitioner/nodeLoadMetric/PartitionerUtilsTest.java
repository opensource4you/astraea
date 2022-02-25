package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class PartitionerUtilsTest {

  @Test
  void testFactorial() {
    assertEquals(factorial(3), 6);
    assertEquals(factorial(5), 120);
  }

  @Test
  void testDoPoisson() {
    assertEquals(doPoisson(10, 15), 0.9512595966960214);
    assertEquals(doPoisson(5, 5), 0.6159606548330632);
    assertEquals(doPoisson(5, 8), 0.9319063652781515);
    assertEquals(doPoisson(5, 2), 0.12465201948308113);
  }

  @Test
  void testSetAllPoisson() {
    HashMap<Integer, Integer> testNodesLoadCount = new HashMap<>();
    testNodesLoadCount.put(0, 2);
    testNodesLoadCount.put(1, 5);
    testNodesLoadCount.put(2, 8);

    HashMap<Integer, Double> poissonMap;
    HashMap<Integer, Double> testPoissonMap = new HashMap<>();
    testPoissonMap.put(0, 0.12465201948308113);
    testPoissonMap.put(1, 0.6159606548330632);
    testPoissonMap.put(2, 0.9319063652781515);

    poissonMap = allPoisson(testNodesLoadCount);

    assertEquals(poissonMap, testPoissonMap);
  }
}
