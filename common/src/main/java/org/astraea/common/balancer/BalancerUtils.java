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
package org.astraea.common.balancer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.metrics.HasBeanObject;

public class BalancerUtils {

  static double evaluateCost(
      ClusterInfo<Replica> clusterInfo,
      Map<HasClusterCost, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        metrics.keySet().stream()
            .map(
                cf -> {
                  var theMetrics = metrics.get(cf);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(cf, cf.clusterCost(clusterInfo, clusterBean).value());
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  /** the lower, the better. */
  static double aggregateFunction(Map<HasClusterCost, Double> scores) {
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }
}
