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

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;

/** Offer a uniform interface to schedule/manage/execute balance plan to an actual Kafka cluster. */
public interface BalancerConsole extends AutoCloseable {

  static BalancerConsole create(Admin admin) {
    return new BalancerConsoleImpl(admin);
  }

  Set<String> tasks();

  Optional<TaskPhase> taskPhase(String taskId);

  Generation launchRebalancePlanGeneration();

  Execution launchRebalancePlanExecution();

  @Override
  void close();

  interface Generation {

    Generation setTaskId(String taskId);

    Generation setBalancer(Balancer balancer);

    Generation setAlgorithmConfig(AlgorithmConfig config);

    Generation setClusterBeanSource(Supplier<ClusterBean> clusterBeanSource);

    Generation checkNoOngoingMigration(boolean enable);

    CompletionStage<Balancer.Plan> generate();
  }

  interface Execution {

    Execution setExecutor(RebalancePlanExecutor executor);

    Execution setExecutionTimeout(Duration timeout);

    Execution checkPlanConsistency(boolean enable);

    Execution checkNoOngoingMigration(boolean enable);

    CompletionStage<Void> execute(String taskId);
  }

  enum TaskPhase implements EnumInfo {
    Searching,
    Searched,
    SearchFailed,
    Executing,
    Executed,
    ExecutionFailed;

    static TaskPhase ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(TaskPhase.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }
}
