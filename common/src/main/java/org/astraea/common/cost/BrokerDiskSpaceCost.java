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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;

public class BrokerDiskSpaceCost implements HasMoveCost {

  public static final String BROKER_COST_LIMIT_KEY = "max.broker.total.disk.space";
  public static final String BROKER_PATH_COST_LIMIT_KEY = "max.broker.path.disk.space";
  private final Map<Integer, DataSize> brokerMoveCostLimit;
  private final Map<BrokerPath, DataSize> diskMoveCostLimit;

  public BrokerDiskSpaceCost(Configuration configuration) {
    this.diskMoveCostLimit = diskMoveCostLimit(configuration);
    this.brokerMoveCostLimit = brokerMoveCostLimit(configuration);
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    if (brokerDiskUsageSizeOverflow(before, after, brokerMoveCostLimit)) return () -> true;
    if (brokerPathDiskUsageSizeOverflow(before, after, diskMoveCostLimit)) return () -> true;
    return () -> false;
  }

  private static Map<BrokerPath, DataSize> diskMoveCostLimit(Configuration configuration) {
    return configuration.list(BROKER_PATH_COST_LIMIT_KEY, ",").stream()
        .collect(
            Collectors.toMap(
                idAndPath -> {
                  var brokerPath = idAndPath.split(":")[0].split("-");
                  return new BrokerPath(
                      Integer.parseInt(brokerPath[0]),
                      IntStream.range(1, brokerPath.length)
                          .boxed()
                          .map(x -> brokerPath[x])
                          .collect(Collectors.joining("-")));
                },
                idAndPath -> DataSize.of(idAndPath.split(":")[1])));
  }

  private Map<Integer, DataSize> brokerMoveCostLimit(Configuration configuration) {
    return configuration.list(BROKER_COST_LIMIT_KEY, ",").stream()
        .collect(
            Collectors.toMap(
                idAndPath -> Integer.parseInt(idAndPath.split(":")[0]),
                idAndPath -> DataSize.of(idAndPath.split(":")[1])));
  }

  static boolean brokerDiskUsageSizeOverflow(
      ClusterInfo before, ClusterInfo after, Map<Integer, DataSize> brokerMoveCostLimit) {
    for (var id :
        Stream.concat(before.brokers().stream(), after.brokers().stream())
            .map(Broker::id)
            .parallel()
            .collect(Collectors.toSet())) {

      var beforeSize = (Long) before.replicaStream(id).map(Replica::size).mapToLong(y -> y).sum();
      var addedSize =
          (Long)
              after
                  .replicaStream(id)
                  .filter(r -> !before.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
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
        var brokerPath = new BrokerDiskSpaceCost.BrokerPath(brokerPaths.getKey(), path);
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
                    .filter(r -> !before.replicas(r.topicPartition()).contains(r))
                    .mapToLong(Replica::size)
                    .sum();
        if ((beforeSize + addedSize)
            > diskMoveCostLimit.getOrDefault(brokerPath, DataSize.Byte.of(Long.MAX_VALUE)).bytes())
          return true;
      }
    }
    return false;
  }

  record BrokerPath(int broker, String path) {}
}
