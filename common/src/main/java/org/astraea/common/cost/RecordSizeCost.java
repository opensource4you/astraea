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

import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;

public class RecordSizeCost
    implements HasClusterCost, HasBrokerCost, HasMoveCost, HasPartitionCost {
  private final Configuration config;
  public static final String MAX_MIGRATE_SIZE_KEY = "maxMigratedSize";

  public RecordSizeCost() {
    this.config = Configuration.of(Map.of());
  }

  public RecordSizeCost(Configuration config) {
    this.config = config;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.nodes().stream()
            .collect(
                Collectors.toMap(
                    NodeInfo::id,
                    n -> clusterInfo.replicaStream(n.id()).mapToDouble(Replica::size).sum()));
    return () -> result;
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var moveCost = ClusterInfo.changedRecordSize(before, after, ignored -> true);
    var maxMigratedSize =
        config
            .string(MAX_MIGRATE_SIZE_KEY)
            .map(DataSize::of)
            .map(DataSize::bytes)
            .orElse(Long.MAX_VALUE);
    var overflow =
        maxMigratedSize
            < moveCost.values().stream()
                .map(DataSize::bytes)
                .map(Math::abs)
                .mapToLong(s -> s)
                .sum();
    return () -> overflow;
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicaLeaders().stream()
            .collect(
                Collectors.groupingBy(
                    Replica::topicPartition,
                    Collectors.mapping(
                        r -> (double) r.size(), Collectors.reducing(0D, Math::max))));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result = clusterInfo.replicaStream().mapToLong(Replica::size).sum();
    return () -> result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  public Configuration config() {
    return this.config;
  }
}
