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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  private Configuration config;
  private static final String UPPER_BOUND = "upper.bound";
  private static final String TRAFFIC_INTERVAL = "traffic.interval";
  private final DataSize upperBound;
  private final DataSize trafficInterval;

  public NetworkIngressCost(Configuration config) {
    super(BandwidthType.Ingress);
    this.config = config;
    this.upperBound = config.dataSize(UPPER_BOUND).orElse(DataSize.MB.of(30));
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
                    replica -> replica.nodeInfo().id(),
                    Collectors.toMap(
                        Replica::topicPartition, r -> partitionTraffic.get(r.topicPartition()))));

    var partitionCost =
        partitionTrafficPerBroker.values().stream()
            .map(
                topicPartitionDoubleMap ->
                    Normalizer.proportion().normalize(topicPartitionDoubleMap))
            .flatMap(cost -> cost.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value() {
        return partitionCost;
      }

      @Override
      public Map<TopicPartition, Set<TopicPartition>> incompatibility() {
        var higherThanUpper = 1;
        var hashMapper =
            (Function<Double, Double>)
                (traffic) -> {
                  if (traffic < upperBound.bytes())
                    return Math.ceil(traffic / trafficInterval.bytes());
                  else
                    return Math.ceil((double) upperBound.bytes() / trafficInterval.bytes())
                        + higherThanUpper;
                };

        var incompatible =
            partitionTrafficPerBroker.values().stream()
                .flatMap(
                    v -> {
                      var groupSimilarTraffic =
                          v.entrySet().stream()
                              .collect(
                                  Collectors.groupingBy(
                                      entry -> hashMapper.apply(entry.getValue()),
                                      Collectors.mapping(
                                          Map.Entry::getKey, Collectors.toUnmodifiableSet())));

                      return v.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey,
                                  e ->
                                      groupSimilarTraffic.values().stream()
                                          .filter(set -> !set.contains(e.getKey()))
                                          .collect(
                                              Collectors.flatMapping(
                                                  Collection::stream,
                                                  Collectors.toUnmodifiableSet()))))
                          .entrySet()
                          .stream();
                    })
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        return incompatible;
      }
    };
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
