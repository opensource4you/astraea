package org.astraea.performance;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistributionTest {

  @Test
  void testFixed() {
    var distribution = Distribution.fixed();
    Assertions.assertEquals(
        1,
        IntStream.range(0, 10)
            .mapToObj(ignored -> distribution.get())
            .collect(Collectors.toSet())
            .size());

    // test setParameters
    distribution.setParameters(List.of("1234"));
    Assertions.assertEquals(1234L, distribution.get());
  }

  @Test
  void testUniform() {
    var distribution = Distribution.uniform(5);
    Assertions.assertTrue(distribution.get() < 5);

    // test setParameters
    distribution.setParameters(List.of("2"));
    Assertions.assertTrue(distribution.get() < 2);
  }

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

    // test setParameters
    distribution.setParameters(List.of("2"));
    Assertions.assertTrue(distribution.get() < 2);
  }
}
