package org.astraea.cost;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class WeightProviderTest {

  @ParameterizedTest
  @MethodSource("normalizers")
  void testEntropy(Normalizer normalizer) {
    var length = 8;
    var numberOfMetrics = 2;
    var numberOfObjects = 2;
    var weightProvider = new WeightProvider.EntropyWeightProvider(normalizer);
    var raw =
        IntStream.range(0, numberOfMetrics)
            .boxed()
            .collect(
                Collectors.toMap(
                    String::valueOf,
                    ignored ->
                        IntStream.range(0, numberOfObjects)
                            .mapToObj(i -> Math.random())
                            .collect(Collectors.toUnmodifiableList())));
    // The smaller entropy means larger weight, so we sort the entropies/weights by different order
    // to check the metrics name later
    var entropies =
        weightProvider.entropies(raw).entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toUnmodifiableList());

    entropies.forEach(e -> Assertions.assertTrue(0 <= e.getValue() && e.getValue() <= 1));

    var weights =
        weightProvider.weight(raw).entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .collect(Collectors.toUnmodifiableList());
    Assertions.assertEquals(entropies.size(), weights.size());
    IntStream.range(0, entropies.size())
        .forEach(i -> Assertions.assertEquals(entropies.get(i).getKey(), weights.get(i).getKey()));
  }

  private static Stream<Arguments> normalizers() {
    return Normalizer.all().stream().map(Arguments::of);
  }
}
