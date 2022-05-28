package org.astraea.cost;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WeightProviderTest {

  @Test
  void testAllSameValues() {
    var weightProvider = WeightProvider.<String, String>entropy(Normalizer.<String>minMax(true));

    Assertions.assertEquals(
        Map.of("throughput", 1D),
        weightProvider.compute(Map.of("throughput", Map.of("n0", 0D, "n1", 0D))));
    Assertions.assertEquals(
        Map.of("throughput", 1D),
        weightProvider.compute(Map.of("throughput", Map.of("n0", 1D, "n1", 1D))));
  }
}
