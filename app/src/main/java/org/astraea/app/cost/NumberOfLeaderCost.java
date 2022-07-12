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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;

/**
 * The result is computed by "LeaderCount.Value". "LeaderCount.Value"" responds to the replica
 * leader number of brokers. The calculation method of the score is the total leader number divided
 * by the total leader number in broker
 */
public class NumberOfLeaderCost implements HasBrokerCost, HasClusterCost {
  Map<Integer, Double> leaderCost;

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(KafkaMetrics.ReplicaManager.LeaderCount::fetch);
  }

  /**
   * @param clusterInfo cluster information
   * @return a BrokerCost contain all ratio of leaders that exist on all brokers
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var leaderCount =
        clusterInfo.clusterBean().all().entrySet().stream()
            .flatMap(
                e ->
                    e.getValue().stream()
                        .filter(x -> x instanceof HasValue)
                        .filter(x -> x.beanObject().properties().get("name").equals("LeaderCount"))
                        .filter(
                            x -> x.beanObject().properties().get("type").equals("ReplicaManager"))
                        .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                        .map(x -> (HasValue) x)
                        .limit(1)
                        .map(e2 -> Map.entry(e.getKey(), (int) e2.value())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var totalLeader = leaderCount.values().stream().mapToInt(Integer::intValue).sum();
    leaderCost =
        leaderCount.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue() / totalLeader));
    return () -> leaderCost;
  }

  @Override
  public ClusterCost clusterCost(
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation) {
    var brokerSizeScore = leaderCost;
    var mean = brokerSizeScore.values().stream().mapToDouble(x -> x).sum() / brokerSizeScore.size();
    var sd =
        brokerSizeScore.values().stream().mapToDouble(score -> Math.sqrt(score - mean)).sum()
            / brokerSizeScore.size();
    return () -> sd;
  }
}
