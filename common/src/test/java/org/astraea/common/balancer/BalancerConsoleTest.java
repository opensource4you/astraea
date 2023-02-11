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
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BalancerConsoleTest {

  @Test
  @Disabled
  void testBalancerConsole() {
    var theAdmin = Mockito.mock(Admin.class);
    var theConfig = AlgorithmConfig.builder().clusterCost((c, b) -> () -> 0D).build();

    // launch rebalance plan generation
    var console = BalancerConsole.create(theAdmin, (x) -> Optional.empty());
    var balanceTask =
        console
            .launchRebalancePlanGeneration()
            .setBalancer(new CustomBalancer(theConfig))
            .setGenerationTimeout(Duration.ofSeconds(1))
            .generate();
    Assertions.assertInstanceOf(String.class, balanceTask.taskId());
    Assertions.assertEquals(BalancerConsole.BalanceTask.Phase.Searching, balanceTask.phase());
    Assertions.assertFalse(balanceTask.planGeneration().toCompletableFuture().isDone());
    Assertions.assertFalse(balanceTask.planExecution().toCompletableFuture().isDone());
    Utils.sleep(Duration.ofSeconds(1).plusMillis(100));
    Assertions.assertEquals(BalancerConsole.BalanceTask.Phase.Searched, balanceTask.phase());
    Assertions.assertTrue(balanceTask.planGeneration().toCompletableFuture().isDone());
    Assertions.assertFalse(balanceTask.planExecution().toCompletableFuture().isDone());

    // this task is there
    Assertions.assertEquals(balanceTask, console.task(balanceTask.taskId()));
    Assertions.assertEquals(Collections.singleton(balanceTask), console.tasks());

    // launch rebalance plan execution
    var customExecutor =
        new RebalancePlanExecutor() {
          @Override
          public CompletionStage<Void> run(
              Admin admin, ClusterInfo targetAllocation, Duration timeout) {
            return CompletableFuture.runAsync(() -> Utils.sleep(timeout));
          }
        };
    var balancerTaskSame =
        console
            .launchRebalancePlanExecution()
            .setExecutionTimeout(Duration.ofSeconds(1))
            .setExecutor(customExecutor)
            .execute(balanceTask);
    Assertions.assertEquals(balanceTask, balancerTaskSame);
    Assertions.assertEquals(BalancerConsole.BalanceTask.Phase.Executing, balanceTask.phase());
    Assertions.assertTrue(balanceTask.planGeneration().toCompletableFuture().isDone());
    Assertions.assertFalse(balanceTask.planExecution().toCompletableFuture().isDone());
    Utils.sleep(Duration.ofSeconds(1).plusMillis(100));
    Assertions.assertEquals(BalancerConsole.BalanceTask.Phase.Executed, balanceTask.phase());
    Assertions.assertTrue(balanceTask.planGeneration().toCompletableFuture().isDone());
    Assertions.assertTrue(balanceTask.planExecution().toCompletableFuture().isDone());

    Assertions.assertDoesNotThrow(console::close);
  }

  @Test
  @Disabled
  void testTasks() {
    var theAdmin = Mockito.mock(Admin.class);
    var theConfig = AlgorithmConfig.builder().clusterCost((c, b) -> () -> 0D).build();

    // launch rebalance plan generation
    var console = BalancerConsole.create(theAdmin, (x) -> Optional.empty());
    var balanceTask0 =
        console
            .launchRebalancePlanGeneration()
            .setBalancer(new CustomBalancer(theConfig))
            .setGenerationTimeout(Duration.ofSeconds(1))
            .generate();
    var balanceTask1 =
        console
            .launchRebalancePlanGeneration()
            .setBalancer(new CustomBalancer(theConfig))
            .setGenerationTimeout(Duration.ofSeconds(1))
            .generate();
    var balanceTask2 =
        console
            .launchRebalancePlanGeneration()
            .setBalancer(new CustomBalancer(theConfig))
            .setGenerationTimeout(Duration.ofSeconds(1))
            .generate();

    Assertions.assertEquals(
        Set.of(balanceTask0, balanceTask1, balanceTask2), Set.copyOf(console.tasks()));
    Assertions.assertEquals(balanceTask0, console.task(balanceTask0.taskId()));
    Assertions.assertEquals(balanceTask1, console.task(balanceTask1.taskId()));
    Assertions.assertEquals(balanceTask2, console.task(balanceTask2.taskId()));
  }

  public static class CustomBalancer implements Balancer {

    public final AlgorithmConfig config;

    public CustomBalancer(AlgorithmConfig config) {
      this.config = config;
    }

    @Override
    public Plan offer(
        ClusterInfo currentClusterInfo,
        ClusterBean clusterBean,
        Duration timeout,
        AlgorithmConfig config) {
      Utils.sleep(timeout);
      return new Plan(() -> 0);
    }
  }
}
