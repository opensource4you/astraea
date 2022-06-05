package org.astraea.cost;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NormalizerTest {

  @ParameterizedTest
  @MethodSource("normalizers")
  void testRandomValues(Normalizer normalizer) {
    IntStream.range(0, 100)
        .forEach(
            index -> {
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

  @ParameterizedTest
  @MethodSource("normalizers")
  void testSmallValues(Normalizer normalizer) {
    IntStream.range(0, 100)
        .forEach(
            index -> {
              // generate random data
              var data =
                  IntStream.range(0, 100)
                      .mapToObj(i -> Math.max(0.3, Math.min(0.7, Math.random())))
                      .collect(Collectors.toUnmodifiableList());
              var result = normalizer.normalize(data);
              Assertions.assertNotEquals(0, result.size());
              // make sure there is no NaN
              result.forEach(v -> Assertions.assertNotEquals(Double.NaN, v));
              // make sure all values are in the range between 0 and 1
              result.forEach(v -> Assertions.assertTrue(0 <= v && v <= 1));
            });
  }

  private static Stream<Arguments> normalizers() {
    return Normalizer.all().stream().map(Arguments::of);
  }
}
