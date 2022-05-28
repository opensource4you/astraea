package org.astraea.cost;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WeightProviderTest {

  @Test
  void testAllSameValues() {
    var weightProvider = WeightProvider.<String, String>entropy(Normalizer.minMax(true));

    Assertions.assertEquals(
        Map.of("throughput", 1D),
        weightProvider.weight(Map.of("throughput", Map.of("n0", 0D, "n1", 0D))));
    Assertions.assertEquals(
        Map.of("throughput", 1D),
        weightProvider.weight(Map.of("throughput", Map.of("n0", 1D, "n1", 1D))));
  }

  @Test
  void testSingleMetrics() {
    var weightProvider = WeightProvider.<String, String>entropy(Normalizer.minMax(true));

    Assertions.assertEquals(
        Map.of("throughput", 1D),
        weightProvider.weight(
            Map.of("throughput", Map.of("n0", Math.random(), "n1", Math.random()))));
  }

  @Test
  void testRange() {
    IntStream.range(0, 100)
        .forEach(
            index -> {
              var weightProvider =
                  WeightProvider.<String, String>entropy(Normalizer.minMax(index % 2 == 0));
              // generate random data
              var data =
                  IntStream.range(0, 100)
                      .mapToObj(String::valueOf)
                      .collect(
                          Collectors.toMap(
                              Function.identity(),
                              ignored ->
                                  IntStream.range(0, 100)
                                      .mapToObj(String::valueOf)
                                      .collect(
                                          Collectors.toMap(
                                              Function.identity(), i -> Math.random()))));
              var result = weightProvider.weight(data);
              Assertions.assertNotEquals(0, result.size());
              // make sure there is no NaN
              result.values().forEach(v -> Assertions.assertNotEquals(Double.NaN, v));
            });
  }

  @Test
  void testEntropy() {
    var weightProvider = WeightProvider.<String, String>entropy(Normalizer.minMax(true));
    // The values in boringData are ordered
    var boringData =
        IntStream.range(0, 10).boxed().collect(Collectors.toMap(String::valueOf, i -> i / 10D));
    // the values in excitingData are chaos (random)
    var excitingData =
        IntStream.range(0, 10)
            .boxed()
            .collect(Collectors.toMap(String::valueOf, i -> Math.random()));

    var result =
        weightProvider.weight(Map.of("boringData", boringData, "excitingData", excitingData));
    Assertions.assertEquals(2, result.size());
    double weightOfBoringData = result.get("boringData");
    double weightOfExcitingData = result.get("excitingData");
    Assertions.assertTrue(
        weightOfExcitingData > weightOfBoringData,
        "weightOfBoringData: "
            + weightOfBoringData
            + " weightOfExcitingData: "
            + weightOfExcitingData
            + " boringData: "
            + boringData
            + " excitingData: "
            + excitingData);
  }
}
