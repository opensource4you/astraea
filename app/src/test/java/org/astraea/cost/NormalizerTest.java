package org.astraea.cost;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NormalizerTest {

  @Test
  void testAllSameValues() {
    var normalizer = Normalizer.<String>minMax(true);
    Assertions.assertEquals(List.of(1D, 1D), normalizer.normalize(List.of(10D, 10D)));
    Assertions.assertEquals(List.of(1D, 1D), normalizer.normalize(List.of(0D, 0D)));
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
                      .map(i -> Math.random() * i * 10000)
                      .collect(Collectors.toUnmodifiableList());
              var result = normalizer.normalize(data);
              Assertions.assertNotEquals(0, result.size());
              // make sure there is no NaN
              result.forEach(v -> Assertions.assertNotEquals(Double.NaN, v));
              // make sure all values are in the range between 0 and 1
              result.forEach(v -> Assertions.assertTrue(0 <= v && v <= 1));
            });
  }
}
