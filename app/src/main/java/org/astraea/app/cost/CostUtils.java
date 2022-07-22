/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.cost;

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

  public static Map<Integer, Double> normalize(Map<Integer, Double> score) {
    var sum = score.values().stream().mapToDouble(d -> d).sum();
    return score.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getKey() / sum));
  }
}
