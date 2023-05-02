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
package org.astraea.common.cost;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.partitioner.PartitionerUtils;

public class LoadCost implements HasBrokerCost {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();
  private final Map<String, Double> metricNameAndWeight =
      Map.of(
          ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.metricName(),
          0.5,
          ServerMetrics.BrokerTopic.BYTES_OUT_PER_SEC.metricName(),
          0.5);

  /** Do "Poisson" and "weightPoisson" calculation on "load". And change output to double. */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var load = computeLoad(clusterBean.all());

    // Poisson calculation (-> Poisson -> throughputAbility -> to double)
    var brokerScore =
        PartitionerUtils.allPoisson(load).entrySet().stream()
            .map(e -> Map.entry(e.getKey(), PartitionerUtils.weightPoisson(e.getValue(), 1.0)))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> brokerScore;
  }

  /**
   * The result is computed by "BytesInPerSec.count" and "BytesOutPerSec.count".
   *
   * <ol>
   *   <li>We normalize the two metric (by divide sum of each metric).
   *   <li>We compute the sum on the two metric with a specific weight.
   *   <li>Compare the weighted sum with two boundary(0.5*avg and 1.5*avg) to get the "load" {0,1,2}
   *   <li>Sum up the historical "load" and divide by 20 the get the score.
   * </ol>
   *
   * <p>e.g. We have 3 brokers with information:<br>
   * broker1: BytesInPerSec.count=50, BytesOutPerSec.count=15 (<1,50000,21000>)<br>
   * broker2: BytesInPerSec.count=100, BytesOutPerSec.count=2 (<2,100000,2000>)<br>
   * broker3: BytesInPerSec.count=200, BytesOutPerSec.count=1 (<3,200000,1000>)<br>
   *
   * <ol>
   *   <li>Normalize: about <1, 1/7, 7/8> <2, 2/7, 1/12> <3, 4/7, 1/24>
   *   <li>WeightedSum: about <1, 57/112> <2, 31/168> <3, 103/336>
   *   <li>Load:<1,2><2,1><3,1>
   * </ol>
   *
   * broker1 score: 2<br>
   * broker2 score: 1<br>
   * broker3 score: 1<br>
   *
   * @return brokerID with "(sum of load)".
   */
  Map<Integer, Integer> computeLoad(Map<Integer, Collection<HasBeanObject>> allBeans) {
    // Store mbean data into local stat("brokersMetric").
    allBeans.forEach(
        (brokerID, value) -> {
          if (!brokersMetric.containsKey(brokerID)) brokersMetric.put(brokerID, new BrokerMetric());
          value.stream()
              .filter(hasBeanObject -> hasBeanObject instanceof ServerMetrics.BrokerTopic.Meter)
              .map(hasBeanObject -> (ServerMetrics.BrokerTopic.Meter) hasBeanObject)
              .forEach(
                  result ->
                      brokersMetric
                          .get(brokerID)
                          .updateCount(
                              result.beanObject().properties().get("name"), result.count()));
        });

    // Sum all count with the same mbean name
    var total =
        brokersMetric.values().stream()
            .map(brokerMetric -> brokerMetric.currentCount)
            .reduce(
                new HashMap<>(),
                (accumulate, currentCount) -> {
                  currentCount.forEach(
                      (name, count) ->
                          accumulate.put(name, accumulate.getOrDefault(name, 0L) + count));
                  return accumulate;
                });

    // Reduce all count for all brokers' mbean current count by "divide total" (current/total). And
    // then get the weighted sum according to predefined wights "metricNameAndWeight".
    // It is called "brokerSituation" in original name.
    var weightedSum =
        brokersMetric.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().currentCount.entrySet().stream()
                            .mapToDouble(
                                nameAndCount ->
                                    (double) (nameAndCount.getValue() + 1)
                                        / (total.getOrDefault(nameAndCount.getKey(), 1L) + 1)
                                        * metricNameAndWeight.getOrDefault(
                                            nameAndCount.getKey(), 0.0))
                            .sum()));

    var avgWeightedSum =
        weightedSum.values().stream().mapToDouble(d -> d).sum() / brokersMetric.size();

    // Record the "load" into local stat("brokersMetric").
    weightedSum.forEach(
        (brokerID, theWeightedSum) -> {
          if (theWeightedSum < avgWeightedSum * 0.5) brokersMetric.get(brokerID).updateLoad(0);
          else if (theWeightedSum < avgWeightedSum * 1.5) brokersMetric.get(brokerID).updateLoad(1);
          else brokersMetric.get(brokerID).updateLoad(2);
        });

    // The sum of historical and current load.
    return brokersMetric.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> entry.getValue().load.stream().mapToInt(i -> i).sum()));
  }

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  @Override
  public Optional<MetricSensor> metricSensor() {
    return Optional.of(
        (client, ignored) ->
            List.of(
                ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.fetch(client),
                ServerMetrics.BrokerTopic.BYTES_OUT_PER_SEC.fetch(client)));
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  private static class BrokerMetric {
    // mbean data. They are:
    // ("BytesInPerSec", BytesInPerSec.count), ("BytesOutPerSec", ByteOutPerSec.count)
    private final Map<String, Long> accumulateCount = new HashMap<>();
    private final Map<String, Long> currentCount = new HashMap<>();

    // Record the latest 10 numbers only.
    private final List<Integer> load =
        IntStream.range(0, 10).mapToObj(i -> 0).collect(Collectors.toList());
    private int loadIndex = 0;

    /**
     * This method records the difference between last update and current given "count" e.g.
     *
     * <p>At time 1: updateCount("ByteInPerSec", 100);<br>
     * At time 3: updateCount("ByteOutPerSec", 20);<br>
     * At time 20: updateCount("ByteInPerSec", 150);<br>
     * At time 22: updateCount("ByteOutPerSec", 90);<br>
     * At time 35: updateCount("ByteInPerSec", 170)<br>
     * Then in time [20, 35), currentCount.get("ByteInPerSec") is 50
     *
     * <p>
     */
    void updateCount(String domainName, Long count) {
      currentCount.put(domainName, count - accumulateCount.getOrDefault(domainName, 0L));
      accumulateCount.put(domainName, count);
    }

    /**
     * This method record input data into a list. This list contains the latest 10 record. Each time
     * it is called, the current index, "loadIndex", is increased by 1.
     */
    void updateLoad(Integer load) {
      this.load.set(loadIndex, load);
      loadIndex = (loadIndex + 1) % 10;
    }
  }
}
