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
package org.astraea.common.partitioner.smooth;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Lazy;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;

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
public final class SmoothWeightRoundRobin {
  private final Lazy<EffectiveWeightResult> effectiveWeightResult = Lazy.of();
  private Map<Integer, Double> currentWeight;
  private final Map<String, List<Integer>> brokersIDofTopic = new HashMap<>();
  private final double upperLimitOffsetRatio = 0.1;

  public SmoothWeightRoundRobin(Map<Integer, Double> effectiveWeight) {
    effectiveWeightResult.get(
        () ->
            new EffectiveWeightResult(
                effectiveWeight.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, ignored -> 1.0))));
    currentWeight =
        effectiveWeight.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, ignored -> 0.0));
  }

  /**
   * Update effective weight.
   *
   * @param brokerScore Broker Score.
   */
  public synchronized void init(Supplier<Map<Integer, Double>> brokerScore) {
    effectiveWeightResult.get(
        () -> {
          var avgScore =
              brokerScore.get().values().stream().mapToDouble(i -> i).average().getAsDouble();
          var offsetRatioOfBroker =
              brokerScore.get().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey, entry -> (entry.getValue() - avgScore) / avgScore));
          // If the average offset of all brokers from the cluster is greater than 0.1, it is
          // unbalanced.
          var balance =
              standardDeviationImperative(avgScore, brokerScore.get())
                  > upperLimitOffsetRatio * avgScore;
          var effectiveWeights =
              this.effectiveWeightResult.get().effectiveWeight.entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey,
                          entry -> {
                            var offsetRatio = offsetRatioOfBroker.get(entry.getKey());
                            var weight =
                                balance ? entry.getValue() * (1 - offsetRatio) : entry.getValue();
                            return Math.max(weight, 0.0);
                          }));
          return new EffectiveWeightResult(effectiveWeights);
        },
        Duration.ofSeconds(10));
  }

  /**
   * Get the preferred ID, and update the state.
   *
   * @return the preferred ID
   */
  public synchronized int getAndChoose(String topic, ClusterInfo<ReplicaInfo> clusterInfo) {
    // TODO Update brokerID with ClusterInfo frequency.
    var brokerID =
        brokersIDofTopic.computeIfAbsent(
            topic,
            e ->
                clusterInfo.replicaLeaders(topic).stream()
                    .map(replicaInfo -> replicaInfo.nodeInfo().id())
                    .collect(Collectors.toList()));
    this.currentWeight =
        this.currentWeight.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        brokerID.contains(e.getKey())
                            ? e.getValue()
                                + effectiveWeightResult.get().effectiveWeight.get(e.getKey())
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
                            ? e.getValue()
                                - effectiveWeightResult.get().effectiveWeightSum(brokerID)
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

  private static double standardDeviationImperative(
      double avgMetrics, Map<Integer, Double> metrics) {
    var variance = new AtomicReference<>(0.0);
    metrics
        .values()
        .forEach(
            metric ->
                variance.updateAndGet(v -> v + (metric - avgMetrics) * (metric - avgMetrics)));
    return Math.sqrt(variance.get() / metrics.size());
  }
}
