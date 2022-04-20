package org.astraea.cost.brokersMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
    var zScore = new HashMap<Integer, Double>();
    metrics
        .entrySet()
        .forEach(
            entry -> {
              var score = ((entry.getValue() - avg) / standardDeviation);
              zScore.put(entry.getKey(), score);
            });
    return zScore;
  }

  public static Map<Integer, Double> TScore(Map<Integer, Double> metrics) {
    var avg = metrics.values().stream().mapToDouble(d -> d).sum() / metrics.size();
    var standardDeviation = standardDeviationImperative(avg, metrics);
    var tScore = new HashMap<Integer, Double>();
    metrics
        .entrySet()
        .forEach(
            entry -> {
              var score = 1 - ((((entry.getValue() - avg) / standardDeviation) * 10 + 50) / 100.0);
              if (score > 1) {
                score = 1.0;
              } else if (score < 0) {
                score = 0.0;
              }
              tScore.put(entry.getKey(), 1.0 - score);
            });
    return tScore;
  }
}
