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
    // 1. get partition byte in per second
    var partitionCost =
        estimateRate(clusterInfo, clusterBean, ServerMetrics.Topic.BYTES_IN_PER_SEC)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()));
    // 2. in order to normalize to [0,1] , calculate the total ingress of the brokers
    var ingressPerBroker =
        clusterInfo
            .replicaStream()
            .filter(Replica::isLeader)
            .filter(Replica::isOnline)
            .collect(
                Collectors.groupingBy(
                    replica -> replica.nodeInfo().id(),
                    Collectors.mapping(
                        replica -> {
                          var tp = replica.topicPartition();
                          partitionLocation.put(tp, replica.nodeInfo().id());
                          return partitionCost.get(tp);
                        },
                        Collectors.summingLong(x -> x))));
    // 3. normalize cost to [0,1]
    var result =
        partitionCost.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        (double) e.getValue()
                            / ingressPerBroker.get(partitionLocation.get(e.getKey()))));
    return () -> result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
