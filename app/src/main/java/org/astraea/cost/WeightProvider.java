package org.astraea.cost;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to provide the weight to score the node or partition
 *
 * @param <Metric> used to represent the "resource". For example, throughput, memory usage, etc.
 * @param <Object> used to represent the "target". For example, broker node, topic partition, etc.
 */
@FunctionalInterface
public interface WeightProvider<Metric, Object> {

  /**
   * create a weight provider based on entropy
   *
   * @param normalizer to normalize input values
   * @param <Metric> metrics type
   * @param <Object> object type
   * @return weight for each metrics
   */
  static <Metric, Object> WeightProvider<Metric, Object> entropy(Normalizer normalizer) {
    // reverse the entropy to simplify following statics
    Function<Collection<Double>, Double> entropy =
        rescaledValues ->
            1
                - rescaledValues.stream()
                        // remove the zero value as it does not influence entropy
                        .filter(value -> value != 0)
                        .mapToDouble(value -> value * Math.log(value))
                        .sum()
                    / (-Math.log(rescaledValues.size()));

    return values -> {
      var entropies =
          values.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e -> entropy.apply(normalizer.normalize(e.getValue().values()))));
      var sum = entropies.values().stream().mapToDouble(d -> d).sum();
      return entropies.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() / sum));
    };
  }

  /**
   * compute the weights for each metric
   *
   * @param values origin data
   * @return metric and its weight
   */
  Map<Metric, Double> weight(Map<Metric, Map<Object, Double>> values);
}
