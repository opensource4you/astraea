package org.astraea.cost;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

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
      double max = values.values().stream().max(comparator).orElse(0.0);
      double min = values.values().stream().min(comparator).orElse(0.0);
      // there is nothing to rescale, so we just all same values
      if (max == min)
        return values.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, ignored -> 1.0));
      return values.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  entry ->
                      (positive ? entry.getValue() - min : max - entry.getValue()) / (max - min)));
    };
  }

  /**
   * rescales the values into a range of [0,1]
   *
   * @param values origin data
   * @return rescaled data
   */
  Map<K, Double> normalize(Map<K, Double> values);
}
