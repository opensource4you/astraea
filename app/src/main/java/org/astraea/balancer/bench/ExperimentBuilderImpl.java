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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ClusterCost;

class ExperimentBuilderImpl implements BalancerBenchmark.ExperimentBuilder {
  private Balancer balancer;
  private ClusterInfo clusterInfo;
  private ClusterBean clusterBean;
  private AlgorithmConfig config;
  private Duration timeout;
  private int trials;

  @Override
  public ExperimentBuilderImpl setBalancer(Balancer balancer) {
    this.balancer = balancer;
    return this;
  }

  @Override
  public ExperimentBuilderImpl setClusterInfo(ClusterInfo clusterInfo) {
    this.clusterInfo = clusterInfo;
    return this;
  }

  @Override
  public ExperimentBuilderImpl setClusterBean(ClusterBean clusterBean) {
    this.clusterBean = clusterBean;
    return this;
  }

  @Override
  public ExperimentBuilderImpl setExecutionTimeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public ExperimentBuilderImpl setAlgorithmConfig(AlgorithmConfig algorithmConfig) {
    this.config = algorithmConfig;
    return this;
  }

  public BalancerBenchmark.ExperimentBuilder setExperimentTrials(int trials) {
    this.trials = trials;
    return this;
  }

  public CompletableFuture<BalancerBenchmark.ExperimentResult> start() {
    final var trials = this.trials;
    final var initial = config.clusterCostFunction().clusterCost(clusterInfo, clusterBean);
    final var results = new ConcurrentLinkedQueue<ClusterCost>();
    final var newConfig =
        AlgorithmConfig.builder(config)
            .clusterInfo(clusterInfo)
            .clusterBean(clusterBean)
            .timeout(timeout)
            .build();
    return CompletableFuture.supplyAsync(
        () -> {
          for (int i = 0; i < trials; i++) {
            balancer
                .offer(newConfig)
                .solution()
                .map(Balancer.Solution::proposalClusterCost)
                .ifPresent(results::add);
          }
          final var res = results.stream().collect(Collectors.toUnmodifiableSet());
          return new BalancerBenchmark.ExperimentResult() {
            @Override
            public ClusterCost initial() {
              return initial;
            }

            @Override
            public Set<ClusterCost> costs() {
              return res;
            }

            @Override
            public int trials() {
              return trials;
            }
          };
        });
  }
}
