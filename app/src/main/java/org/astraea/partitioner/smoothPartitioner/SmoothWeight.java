package org.astraea.partitioner.smoothPartitioner;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.astraea.cost.CostUtils;

/**
 * Given initial key-score pair, it will output a preferred key with higher score. The score of the
 * chosen key will decrease. And the other score will increment by its original score. It may result
 * in "higher score with higher chosen rate".
 */
public final class SmoothWeight {
  private Map<Integer, Double> load;
  private double loadSum;
  public Map<Integer, Double> weight;

  public SmoothWeight() {}

  public SmoothWeight(Map<Integer, Double> load) {
    init(load);
  }

  public synchronized void init(Map<Integer, Double> currentWeight) {
    if (weight == null) {
      this.load = new ConcurrentHashMap<>(currentWeight);
      this.load.replaceAll((k, v) -> 1.0 / currentWeight.size());
      this.weight = new ConcurrentHashMap<>(currentWeight);
      this.weight.replaceAll((k, v) -> 1.0 / weight.size());
      this.loadSum = this.load.values().stream().mapToDouble(d -> d).sum();
    } else {
      System.out.println("cw" + currentWeight);
      var zCurrentLoad = CostUtils.ZScore(currentWeight);
      System.out.println("z:" + zCurrentLoad);
      this.load.replaceAll(
          (k, v) -> {
            var score = v - zCurrentLoad.get(k) * 0.05 / this.load.size();
            if (score > 1.0) {
              return 1.0;
            } else if (score < 0.0) {
              return 0.0;
            }
            return score;
          });
      this.loadSum = this.load.values().stream().mapToDouble(d -> d).sum();
    }
    System.out.println(load);
  }

  /**
   * Get the preferred ID, and update the state.
   *
   * @return the preferred ID
   */
  public int getAndChoose() {
    Objects.requireNonNull(this.weight);

    this.weight.replaceAll((ID, value) -> value + this.load.get(ID));
    var maxID =
        this.weight.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .orElseGet(() -> Map.entry(0, 0.0))
            .getKey();
    this.weight.computeIfPresent(maxID, (ID, value) -> value - loadSum);
    return maxID;
  }

  public Map<Integer, Double> getLoad() {
    Objects.requireNonNull(this.weight);
    this.weight.replaceAll((ID, value) -> value + this.load.get(ID));
    var needLoad = this.weight;
    var maxID =
        this.weight.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .orElseGet(() -> Map.entry(0, 0.0))
            .getKey();
    this.weight.computeIfPresent(maxID, (ID, value) -> value - loadSum);
    return needLoad;
  }
}
