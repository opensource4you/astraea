package org.astraea.cost;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NormalizerTest {

  @Test
  void testAllSameValues() {
    var normalizer = Normalizer.<String>minMax(true);
    Assertions.assertEquals(
        Map.of("s", 1D, "v", 1D), normalizer.normalize(Map.of("s", 10D, "v", 10D)));
    Assertions.assertEquals(
        Map.of("s", 1D, "v", 1D), normalizer.normalize(Map.of("s", 0D, "v", 0D)));
  }
}
