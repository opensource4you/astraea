package org.astraea.cost;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  @Test
  void testRange() {
    IntStream.range(0, 100)
        .forEach(
            index -> {
              var normalizer = Normalizer.<String>minMax(index % 2 == 0);
              // generate random data
              var data =
                  IntStream.range(0, 100)
                      .boxed()
                      .collect(Collectors.toMap(String::valueOf, i -> Math.random() * i * 10000));
              var result = normalizer.normalize(data);
              Assertions.assertNotEquals(0, result.size());
              // make sure there is no NaN
              result.values().forEach(v -> Assertions.assertNotEquals(Double.NaN, v));
              // make sure all values are in the range between 0 and 1
              result.values().forEach(v -> Assertions.assertTrue(0 <= v && v <= 1));
            });
  }
}
