package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestLoadPoisson {
  @Test
  public void testFactorial() {
    LoadPoisson loadPoisson = new LoadPoisson();
    assertEquals(loadPoisson.factorial(3), 6);
    assertEquals(loadPoisson.factorial(5), 120);
  }

  @Test
  public void testDoPoisson() {
    LoadPoisson loadPoisson = new LoadPoisson();
    assertEquals(loadPoisson.doPoisson(10, 15), 0.9512595966960214);
    assertEquals(loadPoisson.doPoisson(5, 5), 0.6159606548330632);
  }
}
