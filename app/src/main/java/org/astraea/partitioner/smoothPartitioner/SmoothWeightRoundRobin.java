package org.astraea.partitioner.smoothPartitioner;

import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.astraea.cost.Periodic;
import org.astraea.cost.brokersMetrics.CostUtils;

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
public final class SmoothWeightRoundRobin extends Periodic<Void> {
  private Map<Integer, Double> effectiveWeight;
  private double effectiveWeightSum;
  public Map<Integer, Double> currentWeight;

  public SmoothWeightRoundRobin() {}

  public SmoothWeightRoundRobin(Map<Integer, Double> effectiveWeight) {
    init(effectiveWeight);
  }

  public synchronized void init(Map<Integer, Double> brokerScore) {
    tryUpdate(
        () -> {
          if (effectiveWeight == null) {
            this.effectiveWeight = new ConcurrentHashMap<>(brokerScore);
            this.effectiveWeight.replaceAll(
                (k, v) -> (double) Math.round(100 * (1.0 / brokerScore.size())) / 100.0);
            this.currentWeight = new ConcurrentHashMap<>(brokerScore);
            this.currentWeight.replaceAll((k, v) -> 0.0);
            this.effectiveWeightSum =
                this.effectiveWeight.values().stream().mapToDouble(i -> i).sum();
          } else {
            var zCurrentLoad = CostUtils.ZScore(brokerScore);
            this.effectiveWeight.replaceAll(
                (k, v) -> {
                  var zLoad = zCurrentLoad.get(k);
                  var score =
                      Math.round(
                              10000
                                  * (v
                                      - (zLoad.isNaN() ? 0.0 : zLoad)
                                          * 0.01
                                          / this.effectiveWeight.size()))
                          / 10000.0;
                  if (score > 1.0) {
                    return 1.0;
                  } else if (score < 0.0) {
                    return 0.0;
                  }
                  return score;
                });
            this.effectiveWeightSum =
                this.effectiveWeight.values().stream().mapToDouble(i -> i).sum();
          }
          return null;
        },
        Duration.ofSeconds(10));
  }

  /**
   * Get the preferred ID, and update the state.
   *
   * @return the preferred ID
   */
  public int getAndChoose() {
    var maxID =
        this.currentWeight.entrySet().stream()
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .orElseGet(() -> Map.entry(0, 0.0))
            .getKey();
    this.currentWeight.computeIfPresent(maxID, (ID, value) -> value - effectiveWeightSum);
    return maxID;
  }
}
