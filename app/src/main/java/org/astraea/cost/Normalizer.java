package org.astraea.cost;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * used to normalize data into a range between [0, 1]
 *
 * @param <K> kind of data. For example, broker data or partition data
 */
public interface Normalizer<K> {
  /**
   * implement the min-max normalization.
   *
   * <p>positive indexes: (value - min) / (max - min) negative
   *
   * <p>indexes: (max - value) / (max - min)
   *
   * @param positive true if the data is positive indexes. Otherwise, false
   * @param <K> kind of data
   * @return min-max normalizer
   */
  static <K> Normalizer<K> minMax(boolean positive) {
    var comparator = Comparator.comparing(Double::doubleValue);
    return values -> {
      double max = values.stream().max(comparator).orElse(0.0);
      double min = values.stream().min(comparator).orElse(0.0);
      // there is nothing to rescale, so we just all same values
      if (max == min)
        return IntStream.range(0, values.size())
            .mapToObj(ignored -> 1.0)
            .collect(Collectors.toUnmodifiableList());
      return values.stream()
          .map(value -> (positive ? value - min : max - value) / (max - min))
          .collect(Collectors.toUnmodifiableList());
    };
  }

  /**
   * rescales the values into a range of [0,1]. this is a helper method to use map collection
   * directly.
   *
   * @param values origin data
   * @return rescaled data
   */
  default Map<K, Double> normalize(Map<K, Double> values) {
    var order = new ArrayList<>(values.entrySet());
    var result =
        normalize(order.stream().map(Map.Entry::getValue).collect(Collectors.toUnmodifiableList()));
    return IntStream.range(0, result.size())
        .boxed()
        .collect(Collectors.toMap(i -> order.get(i).getKey(), result::get));
  }

  /**
   * rescales the values into a range of [0,1]
   *
   * @param values origin data
   * @return rescaled data
   */
  List<Double> normalize(List<Double> values);
}
