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
package org.astraea.common.balancer.algorithms;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.NetworkCost;
import org.astraea.common.cost.NetworkIngressCost;

public class NetworkBalancer implements Balancer {
  @Override
  public Optional<Plan> offer(AlgorithmConfig config) {
    var clusterInfo = config.clusterInfo();
    var clusterBean = config.clusterBean();
    var networkCost = new NetworkIngressCost(Configuration.EMPTY);

    if (clusterInfo.topicPartitions().size() != clusterInfo.replicas().size())
      throw new IllegalArgumentException("NetworkBalancer doesn't support replica");

    NetworkCost.NetworkClusterCost networkClusterCost =
        (NetworkCost.NetworkClusterCost) networkCost.clusterCost(clusterInfo, clusterBean);

    var bandwidths =
        clusterInfo.topicPartitions().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    tp -> tp,
                    tp ->
                        new Bandwidth(
                            networkClusterCost.partitionIngress.get(tp),
                            networkClusterCost.partitionEgress.get(tp))));

    var brokers =
        clusterInfo.brokers().stream()
            .collect(Collectors.toUnmodifiableMap(NodeInfo::id, broker -> new Bandwidth(0, 0)));
    var builder = ClusterInfoBuilder.builder(clusterInfo);

    bandwidths.entrySet().stream()
        .collect(
            Collectors.groupingBy(
                e -> Math.round((double) e.getValue().egress / e.getValue().ingress)))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            e -> {
              var partitions = e.getValue();

              partitions.stream()
                  .sorted(
                      Comparator.<Map.Entry<TopicPartition, Bandwidth>>comparingLong(
                              ee -> ee.getValue().egress)
                          .reversed())
                  .forEach(
                      ee -> {
                        var tp = ee.getKey();
                        var bandwidth = ee.getValue();

                        brokers.entrySet().stream()
                            .min(Comparator.comparingLong(x -> x.getValue().egress))
                            .ifPresent(
                                eee -> {
                                  var toBroker = eee.getKey();
                                  var brokerBandwidth = eee.getValue();

                                  brokerBandwidth.ingress += bandwidth.ingress;
                                  brokerBandwidth.egress += bandwidth.egress;

                                  var replica =
                                      clusterInfo.replicas(tp).get(0).topicPartitionReplica();
                                  var folders =
                                      List.copyOf(clusterInfo.brokerFolders().get(toBroker));
                                  var toFolder =
                                      folders.get(
                                          ThreadLocalRandom.current().nextInt(folders.size()));
                                  builder.reassignReplica(replica, toBroker, toFolder);
                                });
                      });
            });

    var sourceCost = config.clusterCostFunction().clusterCost(clusterInfo, clusterBean);
    var targetClusterInfo = builder.build();
    var targetCost = config.clusterCostFunction().clusterCost(targetClusterInfo, clusterBean);
    var moveCost = config.moveCostFunction().moveCost(clusterInfo, targetClusterInfo, clusterBean);

    System.out.println("NetworkBalancer Initial: " + sourceCost.value());
    System.out.println("NetworkBalancer Final: " + targetCost.value());
    return Optional.of(new Plan(clusterInfo, sourceCost, targetClusterInfo, targetCost, moveCost));
  }

  private static class Bandwidth {
    long ingress;
    long egress;

    public Bandwidth(long ingress, long egress) {
      this.ingress = ingress;
      this.egress = egress;
    }

    @Override
    public String toString() {
      return "Bandwidth{" + "ingress=" + ingress + ", egress=" + egress + '}';
    }
  }
}
