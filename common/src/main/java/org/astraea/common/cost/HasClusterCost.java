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
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.collector.Fetcher;

@FunctionalInterface
public interface HasClusterCost extends CostFunction {

  static HasClusterCost of(Map<HasClusterCost, Double> costAndWeight) {
    var fetcher =
        Fetcher.of(
            costAndWeight.keySet().stream()
                .map(CostFunction::fetcher)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toUnmodifiableList()),
            Throwable::printStackTrace);
    return new HasClusterCost() {
      @Override
      public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
        var cost =
            costAndWeight.entrySet().stream()
                .mapToDouble(
                    cw -> cw.getKey().clusterCost(clusterInfo, clusterBean).value() * cw.getValue())
                .sum();
        return () -> cost;
      }

      @Override
      public Optional<Fetcher> fetcher() {
        return fetcher;
      }
    };
  }

  /**
   * score cluster for a particular metrics according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @param clusterBean cluster metrics
   * @return the score of cluster.
   */
  ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean);
}
