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
package org.astraea.app.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import org.astraea.common.Configuration;
import org.astraea.common.VersionUtils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerProblemFormat;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BalancerBenchmarkAppTest {

  private PrintStream original;
  private ByteArrayOutputStream output = new ByteArrayOutputStream();

  @BeforeEach
  void setOutput() {
    original = System.out;
    System.setOut(new PrintStream(output));
  }

  @AfterEach
  void recoverOutput() {
    System.setOut(original);
  }

  @Test
  void testExecuteHelp() {
    var help = new String[] {"help"};
    var noBench = new String[] {"no_such"};
    Assertions.assertDoesNotThrow(() -> new BalancerBenchmarkApp().execute(help));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new BalancerBenchmarkApp().execute(noBench));
  }

  @Test
  void testExecuteExperiment() {
    var args =
        new BalancerBenchmarkApp.ExperimentArgument() {

          @Override
          ClusterInfo fetchClusterInfo() {
            return ClusterInfo.empty();
          }

          @Override
          ClusterBean fetchClusterBean() {
            return ClusterBean.EMPTY;
          }

          @Override
          String fetchBalancerProblemJson() {
            return "BALANCER_PROBLEM";
          }

          @Override
          BalancerProblemFormat fetchBalancerProblem() {
            var bpf = new BalancerProblemFormat();
            bpf.balancer = NoOpBalancer.class.getName();
            bpf.clusterCosts = List.of(costWeight(ReplicaLeaderCost.class.getName(), 1));
            return bpf;
          }
        };
    args.trials = 1;
    new BalancerBenchmarkApp().runExperiment(args);

    var stdout = output.toString();
    Assertions.assertTrue(stdout.contains("Version: " + VersionUtils.VERSION));
    Assertions.assertTrue(stdout.contains("Balancer: " + NoOpBalancer.class.getName()));
    Assertions.assertTrue(stdout.contains("BALANCER_PROBLEM"));
    Assertions.assertTrue(stdout.contains("Attempted Trials: 1"));
    Assertions.assertTrue(stdout.contains("MOCKED_RESULT"));
  }

  @Test
  void testExecuteCostProfiling() {
    var args =
        new BalancerBenchmarkApp.CostProfilingArgument() {

          @Override
          ClusterInfo fetchClusterInfo() {
            return ClusterInfo.empty();
          }

          @Override
          ClusterBean fetchClusterBean() {
            return ClusterBean.EMPTY;
          }

          @Override
          String fetchBalancerProblemJson() {
            return "BALANCER_PROBLEM";
          }

          @Override
          BalancerProblemFormat fetchBalancerProblem() {
            var bpf = new BalancerProblemFormat();
            bpf.balancer = NoOpBalancer.class.getName();
            bpf.clusterCosts = List.of(costWeight(ReplicaLeaderCost.class.getName(), 1));
            return bpf;
          }
        };
    new BalancerBenchmarkApp().runCostProfiling(args);

    var stdout = output.toString();
    Assertions.assertTrue(stdout.contains("Version: " + VersionUtils.VERSION));
    Assertions.assertTrue(stdout.contains("Balancer: " + NoOpBalancer.class.getName()));
    Assertions.assertTrue(stdout.contains("BALANCER_PROBLEM"));
    Assertions.assertTrue(stdout.contains("MOCKED_RESULT"));

    var matcher = Pattern.compile(": (.+\\.csv)").matcher(stdout);
    Assertions.assertTrue(matcher.find());
    Assertions.assertTrue(Files.exists(Path.of(matcher.group(1))));
    Assertions.assertTrue(matcher.find());
    Assertions.assertTrue(Files.exists(Path.of(matcher.group(1))));
  }

  private static BalancerProblemFormat.CostWeight costWeight(String cost, double weight) {
    var cw = new BalancerProblemFormat.CostWeight();
    cw.cost = cost;
    cw.weight = weight;
    return cw;
  }

  private static class NoOpBalancer implements Balancer {
    public NoOpBalancer(Configuration config) {}

    @Override
    public Optional<Plan> offer(AlgorithmConfig config) {
      return Optional.of(
          new Plan(
              config.clusterInfo(),
              config.clusterCostFunction().clusterCost(config.clusterInfo(), config.clusterBean()),
              config.clusterInfo(),
              ClusterCost.of(ThreadLocalRandom.current().nextDouble(), () -> "MOCKED_RESULT")));
    }
  }
}
