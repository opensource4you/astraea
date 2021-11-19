package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class TestLoadPoisson {
  public NodeLoadClient nodeLoadClient;

  @Test
  public void testFactorial() {
    nodeLoadClient = mock(NodeLoadClient.class);
    LoadPoisson loadPoisson = new LoadPoisson();

    assertEquals(loadPoisson.factorial(3), 6);
    assertEquals(loadPoisson.factorial(5), 120);
  }

  @Test
  public void testDoPoisson() {
    nodeLoadClient = mock(NodeLoadClient.class);
    LoadPoisson loadPoisson = new LoadPoisson();

    assertEquals(loadPoisson.doPoisson(10, 15), 0.9512595966960214);
    assertEquals(loadPoisson.doPoisson(5, 5), 0.6159606548330632);
    assertEquals(loadPoisson.doPoisson(5, 8), 0.9319063652781515);
    assertEquals(loadPoisson.doPoisson(5, 2), 0.12465201948308113);
  }

  @Test
  public void testSetAllPoisson() {
    HashMap<String, Integer> testNodesLoadCount = new HashMap<>();
    testNodesLoadCount.put("0", 10);
    testNodesLoadCount.put("1", 31);
    testNodesLoadCount.put("2", 255);

    HashMap<String, Double> poissonMap;
    HashMap<String, Double> testPoissonMap = new HashMap<>();
    testPoissonMap.put("0", 0.12465201948308113);
    testPoissonMap.put("1", 0.6159606548330632);
    testPoissonMap.put("2", 0.9319063652781515);

    NodeLoadClient nodeLoadClient = mock(NodeLoadClient.class);
    when(nodeLoadClient.getNodeOverLoadCount()).thenReturn(testNodesLoadCount);
    when(nodeLoadClient.getAvgLoadCount()).thenReturn(5);

    LoadPoisson loadPoisson = new LoadPoisson();

    loadPoisson.setAllPoisson(
        nodeLoadClient.getAvgLoadCount(), nodeLoadClient.getNodeOverLoadCount());

    assertEquals(loadPoisson.getAllPoissonMap(), testPoissonMap);
  }
}
