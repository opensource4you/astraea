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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.Fetcher;

public class ReplicaSizeCost
    implements HasMoveCost, HasBrokerCost, HasClusterCost, HasPartitionCost {
  private final Dispersion dispersion = Dispersion.correlationCoefficient();
  public static final String COST_NAME = "size";

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    var removedReplicas = ClusterInfo.diff(before, after);
    var addedReplicas = ClusterInfo.diff(after, before);
    var migrateInfo = migrateInfo(removedReplicas, addedReplicas);
    var sizeChanges = migrateInfo.sizeChange;
    var totalMigrateSize = migrateInfo.totalMigrateSize;
    return new MoveCost() {
      @Override
      public String name() {
        return COST_NAME;
      }

      @Override
      public long totalCost() {
        return totalMigrateSize;
      }

      @Override
      public String unit() {
        return "byte";
      }

      @Override
      public Map<Integer, Long> changes() {
        return sizeChanges;
      }
    };
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the used space for each broker
   */
  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterBean.all().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        LogMetrics.Log.gauges(e.getValue(), LogMetrics.Log.SIZE).stream()
                            .mapToDouble(LogMetrics.Log.Gauge::value)
                            .sum()));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerCost =
        clusterInfo.nodes().stream()
            .collect(
                Collectors.toMap(
                    NodeInfo::id,
                    nodeInfo ->
                        clusterInfo.replicas().stream()
                            .filter(r -> r.nodeInfo().id() == nodeInfo.id())
                            .mapToLong(Replica::size)
                            .sum()));
    var value =
        dispersion.calculate(
            brokerCost.values().stream().map(v -> (double) v).collect(Collectors.toList()));
    return () -> value;
  }

  @Override
  public PartitionCost partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    return () ->
        clusterBean.mapByReplica().entrySet().stream()
            .collect(
                Collectors.toMap(
                    k -> TopicPartition.of(k.getKey().topic(), k.getKey().partition()),
                    e ->
                        LogMetrics.Log.gauges(e.getValue(), LogMetrics.Log.SIZE).stream()
                            .mapToDouble(LogMetrics.Log.Gauge::value)
                            .sum()));
  }

  static class MigrateInfo {
    long totalMigrateSize;
    Map<Integer, Long> sizeChange;

    MigrateInfo(long totalMigrateSize, Map<Integer, Long> sizeChange) {
      this.totalMigrateSize = totalMigrateSize;
      this.sizeChange = sizeChange;
    }
  }

  static MigrateInfo migrateInfo(
      Collection<Replica> removedReplicas, Collection<Replica> addedReplicas) {
    var changes = new HashMap<Integer, Long>();
    AtomicLong totalMigrateSize = new AtomicLong(0L);
    removedReplicas.forEach(
        replica ->
            changes.compute(
                replica.nodeInfo().id(),
                (ignore, size) -> size == null ? -replica.size() : -replica.size() + size));

    addedReplicas.forEach(
        replica -> {
          changes.compute(
              replica.nodeInfo().id(),
              (ignore, size) -> {
                totalMigrateSize.set(totalMigrateSize.get() + replica.size());
                return size == null ? replica.size() : replica.size() + size;
              });
        });
    return new MigrateInfo(totalMigrateSize.get(), changes);
  }
}
