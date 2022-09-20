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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.collector.Fetcher;

/** more replicas migrate -> higher cost */
public class ReplicaNumCost implements HasMoveCost.Helper {

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.empty();
  }

  @Override
  public MoveCost moveCost(
      Collection<Replica> removedReplicas,
      Collection<Replica> addedReplicas,
      ClusterBean clusterBean) {
    return MoveCost.builder()
        .name("Replica Number")
        .unit("replica")
        .totalCost(addedReplicas.size())
        .change(
            Stream.concat(
                    removedReplicas.stream()
                        .map(replica -> Map.entry(replica.nodeInfo().id(), -1L)),
                    addedReplicas.stream().map(replica -> Map.entry(replica.nodeInfo().id(), +1L)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum)))
        .build();
  }
}
