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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

/**
 * This cost will combine multiple ClusterCosts into one ClusterCost. The method of combining will
 * use weighted summation, and the weight will be calculated by passing the brokerCost to
 * WeightProvider#weight.
 */
public class ClusterIntegratedCost implements HasClusterCost {
  static final String UNKNOWN = "Unknown";
  private final List<HasClusterCost> metricsCost =
      List.of(
          new ReplicaSizeCost(),
          new ReplicaLeaderCost(),
          new ReplicaDiskInCost(Configuration.of(Map.of())));
  private final WeightProvider weightProvider = WeightProvider.entropy(Normalizer.minMax(true));

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    Map<String, Collection<Double>> metrics = new HashMap<>();
    metricsCost.forEach(
        hasClusterCost -> {
          var brokerCost = ((HasBrokerCost) hasClusterCost).brokerCost(clusterInfo, clusterBean);
          metrics
              .computeIfAbsent(costName(hasClusterCost), ignore -> new ArrayList<>())
              .addAll(brokerCost.value().values());
        });
    var weight = weightProvider.weight(metrics);
    var clusterCosts =
        metricsCost.stream()
            .collect(
                Collectors.toMap(
                    this::costName,
                    hasClusterCost ->
                        hasClusterCost.clusterCost(clusterInfo, clusterBean).value()));
    var cost =
        clusterCosts.entrySet().stream()
            .mapToDouble(
                clusterCost ->
                    clusterCost.getValue() * weight.getOrDefault(clusterCost.getKey(), 0.0))
            .sum();
    return () -> cost;
  }

  <T> String costName(T cost) {
    if (cost instanceof ReplicaSizeCost) return Costs.ReplicaSizeCost.costName();
    else if (cost instanceof ReplicaLeaderCost) return Costs.ReplicaLeaderCost.costName();
    else if (cost instanceof ReplicaDiskInCost) return Costs.ReplicaDiskInCost.costName();
    return UNKNOWN;
  }

  private enum Costs implements EnumInfo {
    ReplicaSizeCost("ReplicaSizeCost"),
    ReplicaLeaderCost("ReplicaLeaderCost"),
    ReplicaDiskInCost("ReplicaDiskInCost");
    private final String costName;

    Costs(String name) {
      this.costName = name;
    }

    public String costName() {
      return costName;
    }

    public static Costs ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Costs.class, alias);
    }

    @Override
    public String alias() {
      return costName();
    }

    @Override
    public String toString() {
      return alias();
    }
  }
}
