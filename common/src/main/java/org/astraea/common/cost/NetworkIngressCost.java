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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.broker.ServerMetrics;

/**
 * A cost function to evaluate cluster load balance score in terms of message ingress data rate. See
 * {@link NetworkCost} for further detail.
 */
public class NetworkIngressCost extends NetworkCost implements HasPartitionCost {
  private final Configuration config;
  private static final String UPPER_BOUND = "upper.bound";
  private static final String TRAFFIC_INTERVAL = "traffic.interval";
  private long DEFAULT_UPPER_BOUND_BYTES = DataSize.MB.of(30).bytes();
  private long DEFAULT_TRAFFIC_INTERVAL_BYTES = DataSize.MB.of(10).bytes();

  public NetworkIngressCost(Configuration config) {
    super(BandwidthType.Ingress);
    this.config = config;
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    noMetricCheck(clusterBean);

    var partitionTraffic =
        estimateRate(clusterInfo, clusterBean, ServerMetrics.Topic.BYTES_IN_PER_SEC)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    var partitionTrafficPerBroker = wrappedByNode(partitionTraffic, clusterInfo);

    var partitionCost =
        partitionTrafficPerBroker.values().stream()
            .map(
                topicPartitionDoubleMap ->
                    Normalizer.proportion().normalize(topicPartitionDoubleMap))
            .flatMap(cost -> cost.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var partitionCostPerBroker = wrappedByNode(partitionCost, clusterInfo);

    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value() {
        return partitionCost;
      }

      @Override
      public Map<TopicPartition, Set<TopicPartition>> incompatibility() {
        Map<TopicPartition, Set<TopicPartition>> incompatible =
            partitionCost.keySet().stream()
                .map(tp -> Map.entry(tp, new HashSet<TopicPartition>()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        partitionCostPerBroker.forEach(
            (brokerId, partitionCost) -> {
              var partitionTraffic = partitionTrafficPerBroker.get(brokerId);
              var trafficInterval = trafficToCostInterval(partitionTraffic, partitionCost);
              var partitionSet =
                  partitionCost.entrySet().stream()
                      .collect(
                          Collectors.groupingBy(
                              e -> intervalOrder(trafficInterval, e.getValue()),
                              Collectors.mapping(
                                  Map.Entry::getKey, Collectors.toUnmodifiableSet())));

              partitionCost.forEach(
                  (tp, cost) -> {
                    for (var intervals : partitionSet.entrySet()) {
                      if (!intervals.getValue().contains(tp))
                        incompatible.get(tp).addAll(intervals.getValue());
                    }
                  });
            });

        return incompatible;
      }
    };
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  // --------------------[helper]--------------------

  /**
   * assign partition to the number by traffic interval
   *
   * @param interval traffic interval
   * @param value the partition cost
   * @return the number that partition belongs
   */
  private double intervalOrder(List<Double> interval, double value) {
    for (var v : interval) {
      if (value < v) return v;
    }
    return Double.MAX_VALUE;
  }

  /**
   * Obtain the cost for each interval to classify partitions into corresponding intervals for later
   * incompatibility.
   *
   * @param partitionTraffic the traffic of partition in the same node
   * @param partitionCost the cost of partition in the same node
   * @return the interval costs
   */
  private List<Double> trafficToCostInterval(
      Map<TopicPartition, Double> partitionTraffic, Map<TopicPartition, Double> partitionCost) {
    var upperBound =
        convertTrafficToCost(
            partitionTraffic,
            partitionCost,
            config.dataSize(UPPER_BOUND).orElse(DEFAULT_UPPER_BOUND_BYTES));
    var trafficInterval =
        convertTrafficToCost(
            partitionTraffic,
            partitionCost,
            config.dataSize(TRAFFIC_INTERVAL).orElse(DEFAULT_TRAFFIC_INTERVAL_BYTES));
    var count = (int) Math.ceil(upperBound / trafficInterval);
    var largerThanUpperBound = 1;

    return IntStream.range(0, count + largerThanUpperBound)
        .mapToDouble(
            i -> {
              if (i == count) return Double.MAX_VALUE;
              var traffic = trafficInterval * (i + 1);
              return Math.min(traffic, upperBound);
            })
        .boxed()
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Converting partition traffic within the same node into costs
   *
   * @param partitionTraffic partition traffic in the same node
   * @param partitionCost partition cost in the same node
   * @param traffic the traffic would be converted
   * @return the cost derived from traffic conversion
   */
  private double convertTrafficToCost(
      Map<TopicPartition, Double> partitionTraffic,
      Map<TopicPartition, Double> partitionCost,
      double traffic) {
    var trafficCost =
        partitionTraffic.entrySet().stream()
            .filter(e -> e.getValue() > 0.0)
            .findFirst()
            .map(e -> Map.entry(e.getValue(), partitionCost.get(e.getKey())))
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "There is no available traffic, please confirm if the MBean has been retrieved"));
    return traffic / trafficCost.getKey() * trafficCost.getValue();
  }

  /**
   * Group partitions of the same node together
   *
   * @param partitions all partitions in the cluster
   * @param clusterInfo clusterInfo
   * @return A Map with brokerId as the key and partitions as the value
   */
  private Map<Integer, Map<TopicPartition, Double>> wrappedByNode(
      Map<TopicPartition, Double> partitions, ClusterInfo clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .collect(
            Collectors.groupingBy(
                replica -> replica.nodeInfo().id(),
                Collectors.toMap(
                    Replica::topicPartition, r -> partitions.get(r.topicPartition()))));
  }
}
