package org.astraea.partitioner.smoothPartitioner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.astraea.cost.CostUtils;

/**
 * Given initial key-score pair, it will output a preferred key with higher score. The score of the
 * chosen key will decrease. And the other score will increment by its original score. It may result
 * in "higher score with higher chosen rate".
 */
public final class DynamicWeights {
  private Map<Integer, Double> load;

  public DynamicWeights() {}

  public DynamicWeights(Map<Integer, Double> load) {
    init(load);
  }

  public synchronized void init(Map<Integer, Double> currentWeight) {
    if (load == null) {
      this.load = new ConcurrentHashMap<>(currentWeight);
      this.load.replaceAll(
          (k, v) -> (double) Math.round(100 * (1.0 / currentWeight.size())) / 100.0);
    } else {
      var zCurrentLoad = CostUtils.ZScore(currentWeight);
      this.load.replaceAll(
          (k, v) -> {
            var score =
                Math.round(10000 * (v - zCurrentLoad.get(k) * 0.05 / this.load.size())) / 10000.0;
            if (score > 1.0) {
              return 1.0;
            } else if (score < 0.0) {
              return 0.0;
            }
            return score;
          });
    }
  }

  public Map<Integer, Double> getLoad() {
    return this.load;
  }
}
