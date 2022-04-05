package org.astraea.performance;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistributionTest {

  @Test
  void testFixed() {
    var distribution = DistributionType.FIXED.create(new Random().nextInt());
    Assertions.assertEquals(
        1,
        IntStream.range(0, 10)
            .mapToObj(ignored -> distribution.get())
            .collect(Collectors.toSet())
            .size());
  }

  @Test
  void testUniform() {
    var distribution = DistributionType.UNIFORM.create(5);
    Assertions.assertTrue(distribution.get() < 5);
    Assertions.assertTrue(distribution.get() >= 0);
  }

  @Test
  void testLatest() throws InterruptedException {
    var distribution = DistributionType.LATEST.create(Integer.MAX_VALUE);
    Assertions.assertEquals(distribution.get(), distribution.get());

    long first = distribution.get();
    Thread.sleep(2000);

    Assertions.assertNotEquals(first, distribution.get());
  }

  @Test
  void testZipfian() {
    var distribution = DistributionType.ZIPFIAN.create(5);
    Assertions.assertTrue(distribution.get() < 5);
    Assertions.assertTrue(distribution.get() >= 0);
  }
}
