package org.astraea.cost;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@FunctionalInterface
public interface WeightProvider<Metric, Object> {

  static <Metric, Object> WeightProvider<Metric, Object> entropy(Normalizer<Object> normalizer) {
    // reverse the entropy to simplify following statics
    Function<Map<Object, Double>, Double> entropy =
        rescaledValues ->
            1
                - rescaledValues.values().stream()
                        // remove the zero value as it does not influence entropy
                        .filter(v -> v != 0)
                        .mapToDouble(weight -> weight * Math.log(weight))
                        .sum()
                    / (-Math.log(rescaledValues.size()));

    return values -> {
      var entropys =
          values.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, e -> entropy.apply(normalizer.normalize(e.getValue()))));
      var sum = entropys.values().stream().mapToDouble(d -> d).sum();
      return entropys.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() / sum));
    };
  }

  /**
   * compute the weights for each metric
   *
   * @param values origin data
   * @return metric and its weight
   */
  Map<Metric, Double> compute(Map<Metric, Map<Object, Double>> values);
}
