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
import java.util.Collections;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.metrics.collector.MetricSensor;

class CostProfilingImpl implements BalancerBenchmark.CostProfilingBuilder {
  private Balancer balancer;
  private ClusterInfo clusterInfo;
  private ClusterBean clusterBean;
  private AlgorithmConfig config;
  private Duration timeout;

  @Override
  public BalancerBenchmark.CostProfilingBuilder setBalancer(Balancer balancer) {
    this.balancer = balancer;
    return this;
  }

  @Override
  public BalancerBenchmark.CostProfilingBuilder setClusterInfo(ClusterInfo clusterInfo) {
    this.clusterInfo = clusterInfo;
    return this;
  }

  @Override
  public BalancerBenchmark.CostProfilingBuilder setClusterBean(ClusterBean clusterBean) {
    this.clusterBean = clusterBean;
    return this;
  }

  @Override
  public BalancerBenchmark.CostProfilingBuilder setExecutionTimeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public BalancerBenchmark.CostProfilingBuilder setAlgorithmConfig(
      AlgorithmConfig algorithmConfig) {
    this.config = algorithmConfig;
    return this;
  }

  @Override
  public CompletableFuture<BalancerBenchmark.CostProfilingResult> start() {
    final var costFunction = config.clusterCostFunction();
    final var costTimeSeries = new ConcurrentHashMap<Long, ClusterCost>();
    final var clusterCostProcessingTimeNs = new LongSummaryStatistics();
    final var moveCostFunction = config.moveCostFunction();
    final var moveCostTimeSeries = new ConcurrentHashMap<Long, MoveCost>();
    final var moveCostProcessingTimeNs = new LongSummaryStatistics();
    final var newConfig =
        AlgorithmConfig.builder(config)
            .clusterInfo(clusterInfo)
            .clusterBean(clusterBean)
            .timeout(timeout)
            .clusterCost(
                new HasClusterCost() {
                  @Override
                  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
                    final var start = System.nanoTime();
                    final var clusterCost = costFunction.clusterCost(clusterInfo, clusterBean);
                    final var stop = System.nanoTime();
                    costTimeSeries.put(stop, clusterCost);
                    clusterCostProcessingTimeNs.accept((stop - start));
                    return clusterCost;
                  }

                  @Override
                  public Optional<MetricSensor> metricSensor() {
                    return costFunction.metricSensor();
                  }

                  @Override
                  public String toString() {
                    return costFunction.toString();
                  }
                })
            .moveCost(
                new HasMoveCost() {
                  @Override
                  public MoveCost moveCost(
                      ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
                    final var start = System.nanoTime();
                    final var moveCost = moveCostFunction.moveCost(before, after, clusterBean);
                    final var stop = System.nanoTime();
                    moveCostTimeSeries.put(stop, moveCost);
                    moveCostProcessingTimeNs.accept((stop - start));
                    return moveCost;
                  }

                  @Override
                  public Optional<MetricSensor> metricSensor() {
                    return moveCostFunction.metricSensor();
                  }

                  @Override
                  public String toString() {
                    return moveCostFunction.toString();
                  }
                })
            .build();

    return CompletableFuture.supplyAsync(
        () -> {
          var initial = costFunction.clusterCost(clusterInfo, clusterBean);
          var executionStart = System.nanoTime();
          var plan = balancer.offer(newConfig);
          var executionStop = System.nanoTime();

          return new BalancerBenchmark.CostProfilingResult() {
            @Override
            public ClusterCost initial() {
              return initial;
            }

            @Override
            public Optional<Balancer.Solution> solution() {
              return plan.solution();
            }

            @Override
            public Map<Long, ClusterCost> costTimeSeries() {
              return Collections.unmodifiableMap(costTimeSeries);
            }

            @Override
            public Map<Long, MoveCost> moveCostTimeSeries() {
              return Collections.unmodifiableMap(moveCostTimeSeries);
            }

            @Override
            public Duration executionTime() {
              return Duration.ofNanos(executionStop - executionStart);
            }

            @Override
            public LongSummaryStatistics clusterCostProcessingTimeNs() {
              return new LongSummaryStatistics(
                  clusterCostProcessingTimeNs.getCount(),
                  clusterCostProcessingTimeNs.getMin(),
                  clusterCostProcessingTimeNs.getMax(),
                  clusterCostProcessingTimeNs.getSum());
            }

            @Override
            public LongSummaryStatistics moveCostProcessingTimeNs() {
              return new LongSummaryStatistics(
                  moveCostProcessingTimeNs.getCount(),
                  moveCostProcessingTimeNs.getMin(),
                  moveCostProcessingTimeNs.getMax(),
                  moveCostProcessingTimeNs.getSum());
            }
          };
        });
  }
}
