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
package org.astraea.balancer.bench;

import java.time.Duration;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.MoveCost;

public final class BalancerBenchmark {

  /**
   * Execute a balancer multiple times with fixed input. Return the statistical information of this
   * experiment.
   */
  public static ExperimentBuilder experiment() {
    return new ExperimentBuilderImpl();
  }

  /**
   * Execute a balancer once, and profile the proposed cost value change during this process. This
   * experiment tool aims to provide a fine-grain level look at the balancer internal improvement
   * over time.
   */
  public static CostProfilingBuilder costProfiling() {
    return new CostProfilingImpl();
  }

  public interface ExperimentBuilder extends BalancerProblemBuilder<ExperimentBuilder> {

    ExperimentBuilder setExperimentTrials(int trials);

    CompletableFuture<ExperimentResult> start();
  }

  public interface CostProfilingBuilder extends BalancerProblemBuilder<CostProfilingBuilder> {
    CompletableFuture<CostProfilingResult> start();
  }

  public interface BalancerProblemBuilder<T extends BalancerProblemBuilder<T>> {
    T setBalancer(Balancer balancer);

    T setClusterInfo(ClusterInfo clusterInfo);

    T setClusterBean(ClusterBean clusterBean);

    T setExecutionTimeout(Duration timeout);

    T setAlgorithmConfig(AlgorithmConfig algorithmConfig);
  }

  public interface ExperimentResult {

    ClusterCost initial();

    Set<ClusterCost> costs();

    int trials();

    default Optional<ClusterCost> bestCost() {
      return costs().stream().min(Comparator.comparingDouble(ClusterCost::value));
    }

    default OptionalDouble mean() {
      return costs().stream().mapToDouble(ClusterCost::value).average();
    }

    default DoubleSummaryStatistics costSummary() {
      return costs().stream().mapToDouble(ClusterCost::value).summaryStatistics();
    }

    default OptionalDouble variance() {
      if (costs().isEmpty()) return OptionalDouble.empty();

      var summary = costs().stream().mapToDouble(ClusterCost::value).summaryStatistics();
      var mean = summary.getAverage();
      return costs().stream()
          .mapToDouble(ClusterCost::value)
          .map(x -> (x - mean) * (x - mean))
          .average();
    }
  }

  public interface CostProfilingResult {

    ClusterCost initial();

    Optional<Balancer.Solution> solution();

    Map<Long, ClusterCost> costTimeSeries();

    Map<Long, MoveCost> moveCostTimeSeries();

    Duration executionTime();

    LongSummaryStatistics clusterCostProcessingTimeNs();

    LongSummaryStatistics moveCostProcessingTimeNs();
  }
}
