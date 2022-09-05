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
package org.astraea.app.cost;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.metrics.collector.Fetcher;

/** more replicas migrate -> higher cost */
public class ReplicaNumCost implements HasMoveCost {

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.empty();
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> originClusterInfo,
      ClusterInfo<Replica> newClusterInfo,
      ClusterBean clusterBean) {
    var removedReplicas = ClusterInfo.diff(originClusterInfo, newClusterInfo);
    var addedReplicas = ClusterInfo.diff(newClusterInfo, originClusterInfo);
    var migrateInfo = migrateInfo(removedReplicas, addedReplicas);
    var replicaNumChanges = migrateInfo.replicaNumChange;
    var totalReplicaNum = migrateInfo.totalMigrateNum;
    return new MoveCost() {
      @Override
      public String name() {
        return "replica number";
      }

      @Override
      public long totalCost() {
        return totalReplicaNum;
      }

      @Override
      public String unit() {
        return "byte";
      }

      @Override
      public Map<Integer, Long> changes() {
        return replicaNumChanges;
      }
    };
  }

  static class MigrateInfo {
    long totalMigrateNum;
    Map<Integer, Long> replicaNumChange;

    MigrateInfo(long totalMigrateNum, Map<Integer, Long> replicaNumChange) {
      this.totalMigrateNum = totalMigrateNum;
      this.replicaNumChange = replicaNumChange;
    }
  }

  static MigrateInfo migrateInfo(
      Collection<Replica> removedReplicas, Collection<Replica> addedReplicas) {
    var changes = new HashMap<Integer, Long>();
    AtomicLong totalMigrateNum = new AtomicLong(0L);
    removedReplicas.forEach(
        replica ->
            changes.compute(
                replica.nodeInfo().id(), (ignore, size) -> (size == null) ? -1 : size - 1));
    addedReplicas.forEach(
        replica ->
            changes.compute(
                replica.nodeInfo().id(),
                (ignore, size) -> {
                  totalMigrateNum.set(totalMigrateNum.get() + 1);
                  return (size == null) ? 1 : size + 1;
                }));
    return new MigrateInfo(totalMigrateNum.get(), changes);
  }
}
