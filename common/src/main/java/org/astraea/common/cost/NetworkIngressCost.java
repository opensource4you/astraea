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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
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
  public NetworkIngressCost() {
    super(BandwidthType.Ingress);
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    noMetricCheck(clusterBean);

    var partitionLocation = new HashMap<TopicPartition, Integer>();
    var partitionCost =
        estimateRate(clusterInfo, clusterBean, ServerMetrics.Topic.BYTES_IN_PER_SEC)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));

    var partitionPerBroker = new HashMap<Integer, Map<TopicPartition, Double>>();

    clusterInfo.nodes().forEach(node -> partitionPerBroker.put(node.id(), new HashMap<>()));

    clusterInfo
        .replicaStream()
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .forEach(
            replica -> {
              var tp = replica.topicPartition();
              var id = replica.nodeInfo().id();
              partitionPerBroker.get(id).put(tp, partitionCost.get(tp));
            });

    var result =
        partitionPerBroker.values().stream()
            .map(
                topicPartitionDoubleMap ->
                    Normalizer.proportion().normalize(topicPartitionDoubleMap))
            .flatMap(cost -> cost.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return () -> result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
