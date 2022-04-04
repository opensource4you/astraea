package org.astraea.partitioner.smoothPartitioner;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Given initial key-score pair, it will output a preferred key with higher score. The score of the
 * chosen key will decrease. And the other score will increment by its original score. It may result
 * in "higher score with higher chosen rate".
 */
public final class SmoothWeight {
  private Map<Integer, Integer> load;
  private int loadSum;
  public Map<Integer, Integer> weight;

  public SmoothWeight() {}

  public SmoothWeight(Map<Integer, Integer> load) {
    init(load);
  }

  public synchronized void init(Map<Integer, Integer> load) {
    this.load = new HashMap<>(load);
    this.weight = new HashMap<>(load);
    this.loadSum = load.values().stream().mapToInt(d -> d).sum();
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
            .orElseGet(() -> Map.entry(0, 0))
            .getKey();
    this.weight.computeIfPresent(maxID, (ID, value) -> value - loadSum);
    return maxID;
  }
}
