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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost {

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    return brokerCost(
        clusterInfo.topics().stream()
            .flatMap(t -> clusterInfo.availableReplicaLeaders(t).stream()));
  }

  // visible for testing
  BrokerCost brokerCost(Stream<ReplicaInfo> replicas) {
    var result =
        replicas.collect(Collectors.groupingBy(r -> r.nodeInfo().id())).entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> (double) e.getValue().size()));
    return () -> result;
  }
}
