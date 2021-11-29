package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

public class LoadPoissonTest {
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
}
