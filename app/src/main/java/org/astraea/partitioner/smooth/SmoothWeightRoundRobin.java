package org.astraea.partitioner.smooth;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.cost.ClusterInfo;
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
  private Map<Integer, Double> currentWeight;

  private final Map<String, List<Integer>> brokersIDofTopic = new HashMap<>();

  public SmoothWeightRoundRobin(Map<Integer, Double> effectiveWeight) {
    effectiveWeightResult =
        new EffectiveWeightResult(
            effectiveWeight.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, ignored -> 1.0)));
    currentWeight =
        effectiveWeight.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, ignored -> 0.0));
  }

  public synchronized void init(Map<Integer, Double> brokerScore) {
    effectiveWeightResult =
        tryUpdate(
            () -> {
              var normalizationLoad = CostUtils.normalize(brokerScore);
              return new EffectiveWeightResult(
                  this.effectiveWeightResult.effectiveWeight.entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              entry -> entry.getKey(),
                              entry -> {
                                var nLoad = normalizationLoad.get(entry.getKey());
                                var weight =
                                    entry.getValue()
                                        * (nLoad.isNaN()
                                            ? 1.0
                                            : ((nLoad + 1) > 0 ? nLoad + 1 : 0.1));
                                if (weight > 2.0) return 2.0;
                                return Math.max(weight, 0.0);
                              })));
            },
            Duration.ofSeconds(10));
  }

  /**
   * Get the preferred ID, and update the state.
   *
   * @return the preferred ID
   */
  public synchronized int getAndChoose(String topic, ClusterInfo clusterInfo) {
    // TODO Update brokerID with ClusterInfo frequency.
    var brokerID =
        brokersIDofTopic.computeIfAbsent(
            topic,
            e ->
                clusterInfo.availablePartitionLeaders(topic).stream()
                    .map(replicaInfo -> replicaInfo.nodeInfo().id())
                    .collect(Collectors.toList()));
    this.currentWeight =
        this.currentWeight.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        brokerID.contains(e.getKey())
                            ? e.getValue() + effectiveWeightResult.effectiveWeight.get(e.getKey())
                            : e.getValue()));
    var maxID =
        this.currentWeight.entrySet().stream()
            .filter(entry -> brokerID.contains(entry.getKey()))
            .max(Comparator.comparingDouble(Map.Entry::getValue))
            .map(Map.Entry::getKey)
            .orElse(0);
    this.currentWeight =
        this.currentWeight.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getKey().equals(maxID)
                            ? e.getValue() - effectiveWeightResult.effectiveWeightSum(brokerID)
                            : e.getValue()));
    return maxID;
  }

  public static class EffectiveWeightResult {
    private final Map<Integer, Double> effectiveWeight;

    EffectiveWeightResult(Map<Integer, Double> effectiveWeight) {
      this.effectiveWeight = effectiveWeight;
    }

    double effectiveWeightSum(List<Integer> brokerID) {
      return effectiveWeight.entrySet().stream()
          .filter(entry -> brokerID.contains(entry.getKey()))
          .mapToDouble(Map.Entry::getValue)
          .sum();
    }
  }
}
