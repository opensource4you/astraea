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
package org.astraea.app;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.astraea.balancer.bench.BalancerBenchmark;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.NetworkEgressCost;
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.metrics.ClusterBeanSerializer;
import org.astraea.common.metrics.ClusterInfoSerializer;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BalancerExperimentTest {

  public static final String fileName0 = "/home/garyparrot/cluster-file3.bin";
  public static final String fileName1 = "/home/garyparrot/bean-file3.bin";
  public static final String realCluster =
      "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";

  public static void main(String[] args) {
    new BalancerExperimentTest().testProfiling();
  }

  @Disabled
  @Test
  void testProfiling() {
    // load
    try (var admin = Admin.of(realCluster);
        var stream0 = new FileInputStream(fileName0);
        var stream1 = new FileInputStream(fileName1)) {
      // ClusterInfo clusterInfo0 =
      //     admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
      System.out.println("Serialize ClusterInfo");
      ClusterInfo clusterInfo = ClusterInfoSerializer.deserialize(stream0);
      System.out.println("Serialize ClusterBean");
      ClusterBean clusterBean = ClusterBeanSerializer.deserialize(stream1);
      System.out.println("Done!");

      Map<HasClusterCost, Double> costMap =
          Map.of(
              new NetworkIngressCost(Configuration.EMPTY), 3.0,
              new NetworkEgressCost(Configuration.EMPTY), 3.0,
              new ReplicaNumberCost(Configuration.EMPTY), 1.0);
      var costFunction = HasClusterCost.of(costMap);

      var balancer = new GreedyBalancer(Configuration.EMPTY);
      var result =
          BalancerBenchmark.costProfiling()
              .setClusterInfo(clusterInfo)
              .setClusterBean(clusterBean)
              .setBalancer(balancer)
              .setExecutionTimeout(Duration.ofSeconds(60))
              .setAlgorithmConfig(AlgorithmConfig.builder().clusterCost(costFunction).build())
              .start()
              .toCompletableFuture()
              .join();

      var meanClusterCostTime =
          Duration.ofNanos((long) result.clusterCostProcessingTimeNs().getAverage());
      var meanMoveCostTime =
          Duration.ofNanos((long) result.moveCostProcessingTimeNs().getAverage());
      System.out.println("Total Run time: " + result.executionTime().toMillis() + " ms");
      System.out.println(
          "Total ClusterCost Evaluation: " + result.clusterCostProcessingTimeNs().getCount());
      System.out.println(
          "Average ClusterCost Processing: " + meanClusterCostTime.toMillis() + "ms");
      System.out.println("Average MoveCost Processing: " + meanMoveCostTime.toMillis() + "ms");
      System.out.println("Initial Cost: " + result.initial());
      System.out.println(
          "Final Cost: " + result.plan().map(Balancer.Plan::proposalClusterCost).orElse(null));
      var profilingFile = Utils.packException(() -> Files.createTempFile("profile-", ".csv"));
      System.out.println("Profiling File: " + profilingFile.toString());
      System.out.println(
          "Total affected partitions: "
              + ClusterInfo.findNonFulfilledAllocation(
                      clusterInfo, result.plan().orElseThrow().proposal())
                  .size());
      System.out.println();
      try (var stream = Files.newBufferedWriter(profilingFile)) {
        var start = result.costTimeSeries().keySet().stream().mapToLong(x -> x).min().orElseThrow();
        Utils.packException(() -> stream.write("time, cost\n"));
        result.costTimeSeries().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(
                (e) -> {
                  var time = e.getKey();
                  var cost = e.getValue();
                  Utils.packException(
                      () -> stream.write(String.format("%d, %.7f%n", time - start, cost.value())));
                });
      } catch (IOException e) {
        e.printStackTrace();
      }

      System.out.println("Run the plan? (yes/no)");
      while (true) {
        var scanner = new Scanner(System.in);
        String next = scanner.next();
        if (next.equals("yes")) {
          System.out.println("Run the Plan");
          new StraightPlanExecutor(Configuration.EMPTY)
              .run(admin, result.plan().orElseThrow().proposal(), Duration.ofHours(1))
              .toCompletableFuture()
              .join();
          return;
        } else if (next.equals("no")) {
          return;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Disabled
  @Test
  void testSaveScenario() {
    try (Admin admin = Admin.of(realCluster)) {
      var clusterInfo =
          admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
      Map<HasClusterCost, Double> costMap =
          Map.of(
              new NetworkIngressCost(Configuration.EMPTY), 3.0,
              new NetworkEgressCost(Configuration.EMPTY), 3.0,
              new ReplicaNumberCost(Configuration.EMPTY), 1.0);

      try (var metricStore =
          MetricStore.builder()
              .beanExpiration(Duration.ofSeconds(180))
              .sensorsSupplier(
                  () ->
                      costMap.keySet().stream()
                          .filter(x -> x.metricSensor().isPresent())
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  x -> x.metricSensor().orElseThrow(), x -> (i0, i1) -> {})))
              .localReceiver(
                  () ->
                      admin
                          .brokers()
                          .thenApply(
                              (brokers) ->
                                  brokers.stream()
                                      .collect(
                                          Collectors.toUnmodifiableMap(
                                              NodeInfo::id,
                                              (Broker b) -> MBeanClient.jndi(b.host(), 16926)))))
              .build()) {
        var clusterBean = (ClusterBean) null;
        var balancer = new GreedyBalancer(Configuration.EMPTY);

        while (!Thread.currentThread().isInterrupted()) {
          clusterBean = metricStore.clusterBean();

          System.out.println(
              clusterBean.all().entrySet().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> x.getValue().size())));
          try {
            var costFunction = HasClusterCost.of(costMap);
            Optional<Balancer.Plan> offer =
                balancer.offer(
                    AlgorithmConfig.builder()
                        .clusterInfo(clusterInfo)
                        .clusterBean(clusterBean)
                        .clusterCost(costFunction)
                        .timeout(Duration.ofSeconds(10))
                        .build());
            if (offer.isPresent()) {
              System.out.println("Find one");
              break;
            }
          } catch (NoSufficientMetricsException e) {
            System.out.println("No Plan, try later: " + e.getMessage());
            Utils.sleep(Duration.ofSeconds(3));
          }
        }

        // save
        try (var stream0 = new FileOutputStream(fileName0);
            var stream1 = new FileOutputStream(fileName1)) {
          System.out.println("Serialize ClusterInfo");
          ClusterInfoSerializer.serialize(clusterInfo, stream0);
          System.out.println("Serialize ClusterBean");
          ClusterBeanSerializer.serialize(clusterBean, stream1);
        } catch (IOException e) {
          e.printStackTrace();
        }

        // load
        try (var stream0 = new FileInputStream(fileName0);
            var stream1 = new FileInputStream(fileName1)) {
          System.out.println("Serialize ClusterInfo");
          ClusterInfo a = ClusterInfoSerializer.deserialize(stream0);
          System.out.println("Serialize ClusterBean");
          ClusterBean b = ClusterBeanSerializer.deserialize(stream1);
          System.out.println("Done!");
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
