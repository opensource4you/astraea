package org.astraea.cost;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** used to normalize data into a range between [0, 1] */
public interface Normalizer {

  /** @return all normalizers */
  static List<Normalizer> all() {
    return List.of(
        Normalizer.proportion(),
        Normalizer.minMax(true),
        Normalizer.minMax(false),
        Normalizer.TScore());
  }

  /**
   * implement the min-max normalization.
   *
   * <p>positive indexes: (value - min) / (max - min) negative
   *
   * <p>indexes: (max - value) / (max - min)
   *
   * @param positive true if the data is positive indexes. Otherwise, false
   * @return min-max normalizer
   */
  static Normalizer minMax(boolean positive) {
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
   * rescale the value by the proportion
   *
   * <p>value / sum of all values
   *
   * @return proportion normalizer
   */
  static Normalizer proportion() {
    return values -> {
      var sum = values.stream().mapToDouble(i -> i).sum();
      return values.stream().map(v -> v / sum).collect(Collectors.toUnmodifiableList());
    };
  }

  /**
   * implement the TScore normalization
   *
   * <p>(value -avg/standardDeviation)*10+50
   *
   * @return TScore normalizer
   */
  static Normalizer TScore() {
    return values -> {
      var avg = values.stream().mapToDouble(i -> i).sum() / values.size();
      var standardDeviation =
          Math.sqrt(values.stream().mapToDouble(i -> (i - avg) * (i - avg)).sum() / values.size());

      return values.stream()
          .map(i -> (i - avg) / standardDeviation)
          .map(
              i -> {
                var score = (i * 10 + 50) / 100.0;
                if (score > 1) {
                  score = 1.0;
                } else if (score < 0) {
                  score = 0.0;
                }
                return score;
              })
          .collect(Collectors.toUnmodifiableList());
    };
  }

  /**
   * no need normalize
   *
   * @return no normalize
   */
  static Normalizer noNormalize() {
    return values -> values;
  }

  /**
   * rescales the values into a range of [0,1]
   *
   * @param values origin data
   * @return rescaled data
   */
  Collection<Double> normalize(Collection<Double> values);
}
