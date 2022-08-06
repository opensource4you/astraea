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
package org.astraea.app.balancer;

import com.beust.jcommander.Parameter;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.balancer.executor.RebalanceAdmin;
import org.astraea.app.balancer.executor.StraightPlanExecutor;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;

/**
 * A simple demo for Balancer, it does the following:
 *
 * <ol>
 *   <li>Generate a random plan for the target cluster.
 *   <li>Print the information of the generated plan.
 *   <li>Execute the generated plan against the target cluster.
 * </ol>
 */
public class BalanceProcessDemo {

  public static void main(String[] args) {
    var argument = org.astraea.app.argument.Argument.parse(new Argument(), args);
    var rebalancePlanGenerator = new ShufflePlanGenerator(1, 5);
    var rebalancePlanExecutor = new StraightPlanExecutor();
    try (var admin = Admin.of(argument.configs())) {
      Predicate<String> topicFilter = (topic) -> !argument.ignoredTopics.contains(topic);
      var topics =
          admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
      var clusterInfo = admin.clusterInfo(topics);

      // TODO: implement one interface to select the best plan from many plan ,see #544
      var rebalancePlan = rebalancePlanGenerator.generate(clusterInfo).findFirst().orElseThrow();
      System.out.println(rebalancePlan);
      var targetAllocation = rebalancePlan.rebalancePlan();

      System.out.println("[Target Allocation]");
      System.out.println(ClusterLogAllocation.toString(targetAllocation));

      var rebalanceAdmin = RebalanceAdmin.of(admin, topicFilter);
      rebalancePlanExecutor.run(rebalanceAdmin, targetAllocation);
    }
  }

  public static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(names = {"--ignored.topics"})
    public Set<String> ignoredTopics =
        Set.of("__consumer_offsets", "__transaction_state", "__cluster_metadata");
  }
}
