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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;

class CostUtils {

  static boolean brokerDiskUsageSizeOverflow(
      ClusterInfo before, ClusterInfo after, Map<Integer, DataSize> brokerMoveCostLimit) {
    for (var id :
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .parallel()
            .collect(Collectors.toSet())) {

      var beforeSize = (Long) before.replicaStream(id).map(Replica::size).mapToLong(y -> y).sum();
      var addedSize =
          (Long)
              after
                  .replicaStream(id)
                  .filter(r -> before.replicaStream(id).noneMatch(r::equals))
                  .map(Replica::size)
                  .mapToLong(y -> y)
                  .sum();
      if ((beforeSize + addedSize)
          > brokerMoveCostLimit.getOrDefault(id, DataSize.Byte.of(Long.MAX_VALUE)).bytes())
        return true;
    }
    return false;
  }

  static boolean brokerPathDiskUsageSizeOverflow(
      ClusterInfo before,
      ClusterInfo after,
      Map<BrokerDiskSpaceCost.BrokerPath, DataSize> diskMoveCostLimit) {
    for (var brokerPaths :
        Stream.concat(
                before.brokerFolders().entrySet().stream(),
                after.brokerFolders().entrySet().stream())
            .collect(Collectors.toSet())) {
      for (var path : brokerPaths.getValue()) {
        var brokerPath = BrokerDiskSpaceCost.BrokerPath.of(brokerPaths.getKey(), path);
        var beforeSize =
            before
                .replicaStream(brokerPaths.getKey())
                .filter(r -> r.path().equals(path))
                .mapToLong(Replica::size)
                .sum();
        var addedSize =
            (Long)
                after
                    .replicaStream(brokerPaths.getKey())
                    .filter(r -> before.replicaStream(brokerPaths.getKey()).noneMatch(r::equals))
                    .map(Replica::size)
                    .mapToLong(y -> y)
                    .sum();
        if ((beforeSize + addedSize)
            > diskMoveCostLimit.getOrDefault(brokerPath, DataSize.Byte.of(Long.MAX_VALUE)).bytes())
          return true;
      }
    }
    return false;
  }

  static boolean changedRecordSizeOverflow(
      ClusterInfo before, ClusterInfo after, Predicate<Replica> predicate, long limit) {
    var totalRemovedSize = 0L;
    var totalAddedSize = 0L;
    for (var id :
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .parallel()
            .collect(Collectors.toSet())) {
      var removed =
          (int)
              before
                  .replicaStream(id)
                  .filter(predicate)
                  .filter(r -> !after.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
                  .sum();
      var added =
          (int)
              after
                  .replicaStream(id)
                  .filter(predicate)
                  .filter(r -> !before.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
                  .sum();
      totalRemovedSize = totalRemovedSize + removed;
      totalAddedSize = totalAddedSize + added;
      // if migrate cost overflow, leave early and return true
      if (totalRemovedSize > limit || totalAddedSize > limit) return true;
    }
    return Math.max(totalRemovedSize, totalAddedSize) > limit;
  }
}
