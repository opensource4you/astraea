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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.broker.ServerMetrics;

/**
 * A cost function to evaluate cluster load balance score in terms of message ingress data rate. See
 * {@link NetworkCost} for further detail.
 */
public class NetworkIngressCost extends NetworkCost implements HasPartitionCost {
  private Configuration config;
  public static final String TRAFFIC_INTERVAL = "traffic.interval";
  private final DataSize trafficInterval;

  public NetworkIngressCost(Configuration config) {
    super(config, BandwidthType.Ingress);
    this.config = config;
    this.trafficInterval = config.dataSize(TRAFFIC_INTERVAL).orElse(DataSize.MB.of(10));
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    noMetricCheck(clusterBean);

    var partitionTraffic =
        estimateRate(clusterInfo, clusterBean, ServerMetrics.Topic.BYTES_IN_PER_SEC)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));

    var partitionTrafficPerBroker =
        clusterInfo
            .replicaStream()
            .filter(Replica::isLeader)
            .filter(Replica::isOnline)
            .collect(
                Collectors.groupingBy(
                    replica -> replica.broker().id(),
                    Collectors.toMap(
                        Replica::topicPartition, r -> partitionTraffic.get(r.topicPartition()))));

    var partitionCost =
        partitionTrafficPerBroker.values().stream()
            .map(
                topicPartitionDoubleMap ->
                    Normalizer.proportion().normalize(topicPartitionDoubleMap))
            .flatMap(cost -> cost.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var avg = partitionTraffic.values().stream().mapToDouble(i -> i).average().orElse(0.0);
    var standardDeviation =
        Math.sqrt(
            partitionTraffic.values().stream()
                .mapToDouble(i -> Math.pow(i - avg, 2))
                .average()
                .getAsDouble());
    var upperBound =
        partitionTraffic.values().stream()
            .filter(v -> v < standardDeviation)
            .max(Comparator.naturalOrder())
            .orElse(avg);

    // Calculate partitions that are not suitable to be assigned together based on the partition
    // traffic
    // 1. Calculate the standard deviation of the partition traffics on each node
    // 2. Find the maximum value less than the standard deviation, we call the value `upper
    // bound`
    // 3. If the traffic of the partition is greater than the `upper bound`, the partition isn't
    // suitable to be assigned together with partitions that have traffic lower than the upper
    // bound
    // 4. When the traffic of the partition is less than the `upper bound`, the partition isn't
    // suitable to be assigned together with partitions that have the difference value higher
    // than traffic interval
    var incompatible =
        partitionTrafficPerBroker.values().stream()
            .flatMap(
                tpTraffic ->
                    tpTraffic.entrySet().stream()
                        .map(
                            tp ->
                                tp.getValue() < upperBound
                                    ? Map.entry(
                                        tp.getKey(),
                                        tpTraffic.entrySet().stream()
                                            .filter(
                                                others -> // using traffic interval to filter
                                                Math.abs(tp.getValue() - others.getValue())
                                                        > trafficInterval.bytes())
                                            .map(Map.Entry::getKey)
                                            .collect(Collectors.toUnmodifiableSet()))
                                    : Map.entry(
                                        tp.getKey(),
                                        tpTraffic.entrySet().stream()
                                            .filter(others -> others.getValue() < upperBound)
                                            .map(Map.Entry::getKey)
                                            .collect(Collectors.toUnmodifiableSet()))))
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value() {
        return partitionCost;
      }

      @Override
      public Map<TopicPartition, Set<TopicPartition>> incompatibility() {
        return incompatible;
      }
    };
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
