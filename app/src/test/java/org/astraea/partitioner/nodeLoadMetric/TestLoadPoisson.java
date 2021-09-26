package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class TestLoadPoisson {
  public NodeLoadClient nodeLoadClient;

  @Test
  public void testFactorial() {
    nodeLoadClient = mock(NodeLoadClient.class);
    LoadPoisson loadPoisson = new LoadPoisson(nodeLoadClient);

    assertEquals(loadPoisson.factorial(3), 6);
    assertEquals(loadPoisson.factorial(5), 120);
  }

  @Test
  public void testDoPoisson() {
    nodeLoadClient = mock(NodeLoadClient.class);
    LoadPoisson loadPoisson = new LoadPoisson(nodeLoadClient);

    assertEquals(loadPoisson.doPoisson(10, 15), 0.9512595966960214);
    assertEquals(loadPoisson.doPoisson(5, 5), 0.6159606548330632);
    assertEquals(loadPoisson.doPoisson(5, 8), 0.9319063652781515);
    assertEquals(loadPoisson.doPoisson(5, 2), 0.12465201948308113);
  }

  @Test
  public void testSetAllPoisson() {
    HashMap<Integer,Integer> testNodesLoadCount = new HashMap<>();
    testNodesLoadCount.put(0, 10);
    testNodesLoadCount.put(1, 31);
    testNodesLoadCount.put(2, 255);

    HashMap<Integer, Double> poissonMap;
    HashMap<Integer, Double> testPoissonMap = new HashMap<>();
    testPoissonMap.put(0, 0.12465201948308113);
    testPoissonMap.put(1, 0.6159606548330632);
    testPoissonMap.put(2, 0.9319063652781515);

    NodeLoadClient nodeLoadClient = mock(NodeLoadClient.class,
            withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));

    nodeLoadClient.setOverLoadCount(testNodesLoadCount);

    LoadPoisson loadPoisson = new LoadPoisson(nodeLoadClient);

    poissonMap = loadPoisson.setAllPoisson();

    assertEquals(poissonMap, testPoissonMap);
  }
}
