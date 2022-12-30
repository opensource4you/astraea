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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.collector.Fetcher;

@FunctionalInterface
public interface HasPartitionCost extends CostFunction {

  HasPartitionCost EMPTY = (clusterInfo, clusterBean) -> Map::of;

  static HasPartitionCost of(Map<HasPartitionCost, Double> costAndWeight) {
    var fetcher =
        Fetcher.of(
            costAndWeight.keySet().stream()
                .map(CostFunction::fetcher)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()));
    return new HasPartitionCost() {
      @Override
      public PartitionCost partitionCost(
          ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
        var result = new HashMap<TopicPartition, Double>();
        costAndWeight.forEach(
            (function, weight) ->
                function
                    .partitionCost(clusterInfo, clusterBean)
                    .value()
                    .forEach(
                        (tp, v) ->
                            result.compute(
                                tp,
                                (ignore, previous) ->
                                    previous == null ? v * weight : v * weight + previous)));
        return () -> Collections.unmodifiableMap(result);
      }

      @Override
      public Optional<Fetcher> fetcher() {
        return fetcher;
      }
    };
  }

  PartitionCost partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean);
}
