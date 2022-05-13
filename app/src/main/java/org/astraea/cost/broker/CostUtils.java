package org.astraea.cost.broker;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CostUtils {
  public static double standardDeviationImperative(
      double avgMetrics, Map<Integer, Double> metrics) {
    var variance = new AtomicReference<>(0.0);
    metrics
        .values()
        .forEach(
            metric ->
                variance.updateAndGet(v -> v + (metric - avgMetrics) * (metric - avgMetrics)));
    return Math.sqrt(variance.get() / metrics.size());
  }

  public static Map<Integer, Double> ZScore(Map<Integer, Double> metrics) {
    var avg = metrics.values().stream().mapToDouble(d -> d).sum() / metrics.size();
    var standardDeviation = standardDeviationImperative(avg, metrics);
    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey, e -> (e.getValue() - avg) / standardDeviation));
  }

  public static Map<Integer, Double> TScore(Map<Integer, Double> metrics) {
    var avg = metrics.values().stream().mapToDouble(d -> d).sum() / metrics.size();
    var standardDeviation = standardDeviationImperative(avg, metrics);

    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  var score =
                      Math.round((((e.getValue() - avg) / standardDeviation) * 10 + 50)) / 100.0;
                  if (score > 1) {
                    score = 1.0;
                  } else if (score < 0) {
                    score = 0.0;
                  }
                  return score;
                }));
  }
}
