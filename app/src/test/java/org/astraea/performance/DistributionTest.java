package org.astraea.performance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistributionTest {
  @Test
  void testLatest() throws InterruptedException {
    var distribution = Distribution.latest();
    Assertions.assertEquals(distribution.get(), distribution.get());

    long first = distribution.get();
    Thread.sleep(2000);

    Assertions.assertNotEquals(first, distribution.get());
  }

  @Test
  void testZipfian() {
    var distribution = Distribution.zipfian(5);
    Assertions.assertTrue(distribution.get() < 5);
  }
}
