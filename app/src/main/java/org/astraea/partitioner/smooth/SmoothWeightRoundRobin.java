package org.astraea.partitioner.smooth;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.astraea.cost.Periodic;
import org.astraea.cost.broker.CostUtils;

/**
 * Given initial key-score pair, it will output a preferred key with the highest current weight. The
 * current weight of the chosen key will decrease the sum of effective weight. And all current
 * weight will increment by its effective weight. It may result in "higher score with higher chosen
 * rate". For example:
 *
 * <p>||========================================||============================================||
 * ||------------ Broker in cluster ------------||------------- Effective weight -------------||
 * ||------------------Broker1------------------||-------------------- 5 ---------------------||
 * ||------------------Broker2------------------||-------------------- 1 ---------------------||
 * ||------------------Broker3------------------||-------------------- 1 ---------------------||
 * ||===========================================||============================================||
 *
 * <p>||===================||=======================||===============||======================||
 * ||--- Request Number ---|| Before current weight || Target Broker || After current weight ||
 * ||----------1-----------||------ {5, 1, 1} ------||----Broker1----||----- {-2, 1, 1} -----||
 * ||----------2-----------||------ {3, 2, 2} ------||----Broker1----||----- {-4, 2, 2} -----||
 * ||----------3-----------||------ {1, 3, 3} ------||----Broker2----||----- { 1,-4, 3} -----||
 * ||----------4-----------||------ {6,-3, 4} ------||----Broker1----||----- {-1,-3, 4} -----||
 * ||----------5-----------||------ {4,-2, 5} ------||----Broker3----||----- { 4,-2,-2} -----||
 * ||----------6-----------||------ {9,-1,-1} ------||----Broker1----||----- { 2,-1,-1} -----||
 * ||----------7-----------||------ {7, 0, 0} ------||----Broker1----||----- { 0, 0, 0} -----||
 * ||======================||=======================||===============||======================||
 */
public final class SmoothWeightRoundRobin
    extends Periodic<SmoothWeightRoundRobin.EffectiveWeightResult> {
  private EffectiveWeightResult effectiveWeightResult;
  public Map<Integer, Double> currentWeight;

  public SmoothWeightRoundRobin(Map<Integer, Double> effectiveWeight) {
    init(effectiveWeight);
  }

  public synchronized void init(Map<Integer, Double> brokerScore) {
    tryUpdate(
        () -> {
          Map<Integer, Double> effectiveWeight;
          if (effectiveWeightResult == null) {
            effectiveWeight = new HashMap<>(brokerScore);
            effectiveWeight.replaceAll((k, v) -> 1.0);
            this.currentWeight = new HashMap<>(brokerScore);
            this.currentWeight.replaceAll((k, v) -> 0.0);
          } else {
            var normalizationLoad = CostUtils.normalize(brokerScore);
            effectiveWeight = this.effectiveWeightResult.effectiveWeight;
            effectiveWeight.replaceAll(
                (k, v) -> {
                  var nLoad = normalizationLoad.get(k);
                  var weight = v * (nLoad.isNaN() ? 1.0 : ((nLoad + 1) > 0 ? nLoad + 1 : 0.1));
                  if (weight > 2.0) {
                    return 2.0;
                  } else if (weight < 0.0) {
                    return 0.0;
                  }
                  return weight;
                });
          }
          return new EffectiveWeightResult(effectiveWeight);
        },
        10);
  }

  /**
   * Get the preferred ID, and update the state.
   *
   * @return the preferred ID
   */
  public synchronized int getAndChoose() {
    var effective = effectiveWeightResult.effectiveWeight;
    this.currentWeight.replaceAll((k, v) -> v + effective.get(k));
    var maxID =
        this.currentWeight.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .orElseGet(() -> Map.entry(0, 0.0))
            .getKey();
    this.currentWeight.computeIfPresent(
        maxID, (ID, value) -> value - effectiveWeightResult.effectiveWeightSum);
    return maxID;
  }

  public static class EffectiveWeightResult {
    private final Map<Integer, Double> effectiveWeight;
    private final double effectiveWeightSum;

    EffectiveWeightResult(Map<Integer, Double> effectiveWeight) {
      this.effectiveWeight = effectiveWeight;
      this.effectiveWeightSum = effectiveWeight.values().stream().mapToDouble(i -> i).sum();
    }
  }
}
