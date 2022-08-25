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
package org.astraea.app.web;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.balancer.BalancerUtils;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.HasClusterCost;
import org.astraea.app.cost.ReplicaLeaderCost;

class BalancerHandler implements Handler {

  static String LIMIT_KEY = "limit";

  static int LIMIT_DEFAULT = 10000;
  private final Admin admin;
  private final RebalancePlanGenerator generator = RebalancePlanGenerator.random(30);
  private final HasClusterCost costFunction;

  BalancerHandler(Admin admin) {
    this(admin, new ReplicaLeaderCost());
  }

  BalancerHandler(Admin admin, HasClusterCost costFunction) {
    this.admin = admin;
    this.costFunction = costFunction;
  }

  @Override
  public Response get(Channel channel) {
    var clusterInfo = admin.clusterInfo();
    var clusterAllocation = ClusterLogAllocation.of(clusterInfo);
    var cost = costFunction.clusterCost(clusterInfo, ClusterBean.EMPTY).value();
    var limit =
        Integer.parseInt(channel.queries().getOrDefault(LIMIT_KEY, String.valueOf(LIMIT_DEFAULT)));
    var planAndCost =
        generator
            .generate(admin.brokerFolders(), clusterAllocation)
            .limit(limit)
            .map(RebalancePlanProposal::rebalancePlan)
            .map(
                cla ->
                    Map.entry(
                        cla,
                        costFunction
                            .clusterCost(BalancerUtils.merge(clusterInfo, cla), ClusterBean.EMPTY)
                            .value()))
            .filter(e -> e.getValue() <= cost)
            .min(Comparator.comparingDouble(Map.Entry::getValue));

    return new Report(
        cost,
        planAndCost.map(Map.Entry::getValue).orElse(cost),
        limit,
        costFunction.getClass().getSimpleName(),
        planAndCost
            .map(
                entry ->
                    ClusterLogAllocation.findNonFulfilledAllocation(
                            clusterAllocation, entry.getKey())
                        .stream()
                        .map(
                            tp ->
                                new Change(
                                    tp.topic(),
                                    tp.partition(),
                                    placements(clusterAllocation.logPlacements(tp)),
                                    placements(entry.getKey().logPlacements(tp))))
                        .collect(Collectors.toUnmodifiableList()))
            .orElse(List.of()));
  }

  static List<Placement> placements(List<LogPlacement> lps) {
    return lps.stream().map(Placement::new).collect(Collectors.toUnmodifiableList());
  }

  static class Placement {

    final int brokerId;
    final String directory;

    Placement(LogPlacement lp) {
      this.brokerId = lp.broker();
      this.directory = lp.logDirectory();
    }
  }

  static class Change {
    final String topic;
    final int partition;
    final List<Placement> before;
    final List<Placement> after;

    Change(String topic, int partition, List<Placement> before, List<Placement> after) {
      this.topic = topic;
      this.partition = partition;
      this.before = before;
      this.after = after;
    }
  }

  static class Report implements Response {
    final double cost;
    final double newCost;

    final int limit;

    final String function;
    final List<Change> changes;

    Report(double cost, double newCost, int limit, String function, List<Change> changes) {
      this.cost = cost;
      this.newCost = newCost;
      this.limit = limit;
      this.function = function;
      this.changes = changes;
    }
  }
}
